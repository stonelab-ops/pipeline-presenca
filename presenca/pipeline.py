# presenca/pipeline.py

import logging
from typing import Dict, Any
from .utils.data_reader import DataReader
from .utils.data_writer import DataWriter
from .domain.factory import TenureFactory
from .domain.services.data_processing import DataProcessingService
from .domain.services.base_report_builder import BaseReportBuilder
from .domain.services.weekly_report_enhancer import WeeklyReportEnhancer
from .domain.services.kpi_calculator import KpiCalculator
from .domain.services.report_generators.action_sheets import ActionSheetGenerator
from .domain.services.report_generators.summary_sheet import SummarySheetGenerator
from .domain.services.report_generators.kpi_sheets import KpiSheetGenerator

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
log = logging.getLogger(__name__)

class PresencePipeline:
    """
    Orquestrador principal que define a sequência de operações 
    para gerar os relatórios de presença.
    """
    def __init__(self, data_reader: DataReader, data_writer: DataWriter, config: Dict[str, Any]):
        self.data_reader = data_reader
        self.data_writer = data_writer
        self.config = config
        
        self.tenure_factory = TenureFactory()
        log.info("Pipeline de Presença inicializado.")

    def run(self) -> str:
        """
        Executa o pipeline completo de processamento e geração de relatórios.
        """
        try:
            log.info("Iniciando 1/5: Leitura de Dados...")
            all_data = self.data_reader.load_all_sources()
            
            log.info("Iniciando 2/5: Processamento de Dados...")
            tenures = self.tenure_factory.create_tenures_from_df(all_data['io_alunos'])
            
            processor_service = DataProcessingService(all_data, self.config)
            processed_data = processor_service.run()

            log.info("Iniciando 3/5: Construção do Relatório Base...")
            
            base_builder = BaseReportBuilder(self.config)
            base_report = base_builder.build(
                active_students=processed_data['registros_final'], 
                tenures=tenures
            )

            enhancer = WeeklyReportEnhancer()
            weekly_report = enhancer.enhance(
                base_report=base_report,
                attendance=processed_data['registros_final'],
                student_info=processed_data['cadastro'],
                holidays_df=processed_data['feriados'],
                justifications_df=processed_data['justificativas'],
                tenures=tenures
            )
            
            calculator = KpiCalculator(weekly_report, processed_data, self.config)
            report_with_kpis = calculator.calculate()
            
            log.info("Relatório base com KPIs concluído.")

            log.info("Iniciando 4/5: Geração das Abas de Saída...")
            
            action_gen = ActionSheetGenerator(processed_data, self.config)
            
            # --- LINHA CORRIGIDA ABAIXO ---
            summary_gen = SummarySheetGenerator(report_with_kpis, self.config)
            # --- FIM DA CORREÇÃO ---
            
            kpi_gen = KpiSheetGenerator(report_with_kpis)

            final_tabs = {}
            final_tabs.update(action_gen.generate())
            final_tabs.update(summary_gen.generate())
            final_tabs.update(kpi_gen.generate())
            
            log.info(f"Abas geradas: {list(final_tabs.keys())}")

            log.info("Iniciando 5/5: Escrita do Arquivo Excel...")
            output_file_path = self.data_writer.save_report_to_excel(
                report_tabs=final_tabs,
                base_filename="relatorio_presenca_stonelab"
            )
            
            log.info(f"--- SUCESSO! Pipeline concluído. ---")
            log.info(f"Arquivo de saída salvo em: {output_file_path}")
            
            return output_file_path

        except Exception as e:
            log.error(f"--- FALHA NO PIPELINE: {e} ---", exc_info=True)
            return ""