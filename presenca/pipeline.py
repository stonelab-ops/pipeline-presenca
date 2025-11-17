import logging
from typing import Dict, Any
from .utils.data_reader import DataReader
from .utils.data_writer import DataWriter
from .domain.factory import TenureFactory
from .domain.services.data_processing import DataProcessingService
from .domain.services.base_report_builder import BaseReportBuilder
from .domain.services.weekly_report_enhancer import WeeklyReportEnhancer
from .domain.services.kpi_calculator_padrao import KpiCalculatorPadrao
from .domain.services.report_generators.action_sheets import ActionSheetGenerator
from .domain.services.report_generators.summary_sheet import SummarySheetGenerator
from .domain.services.report_generators.kpi_sheets import KpiSheetGenerator
import pandas as pd
import calendar
from datetime import datetime

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
log = logging.getLogger(__name__)

class PresencePipeline:
    
    def __init__(self, data_reader: DataReader, data_writer: DataWriter, config: object):
        self.data_reader = data_reader
        self.data_writer = data_writer
        self.config = config
        
        self.tenure_factory = TenureFactory()
        log.info("Pipeline de Presença: Iniciando execução.")

    def run(self) -> str:
        
        try:
            try:
                ano = self.config.ANO_DO_RELATORIO
                mes = self.config.MES_DO_RELATORIO
                data_inicio = datetime(ano, mes, 1).date()
                _, ultimo_dia = calendar.monthrange(ano, mes)
                data_fim = datetime(ano, mes, ultimo_dia).date()
                
                self.config.DATA_INICIO_GERAL = data_inicio.strftime('%Y-%m-%d')
                self.config.DATA_FIM_GERAL = data_fim.strftime('%Y-%m-%d')
            except Exception as e:
                log.error(f"Falha ao calcular datas do relatório: {e}")
                raise

            log.info("Leitura: Carregando fontes de dados (XMLs e Planilhas)...")
            all_data = self.data_reader.load_all_sources()
            
            log.info("Processamento: Limpando e preparando dados brutos...")
            tenures = self.tenure_factory.create_tenures_from_df(all_data['io_alunos'])
            
            processor_service = DataProcessingService(all_data, self.config)
            processed_data = processor_service.run()

            log.info("Construção: Gerando relatório base semanal...")
            
            df_cadastro_completo = processed_data['cadastro']
            
            if not processed_data['registros_final'].empty:
                ids_com_presenca = processed_data['registros_final']['id_stonelab'].unique()
                df_alunos_ativos_para_relatorio = df_cadastro_completo[
                    df_cadastro_completo['id_stonelab'].isin(ids_com_presenca)
                ].copy()
            else:
                log.warning("Nenhum registro final encontrado. O relatório base estará vazio.")
                df_alunos_ativos_para_relatorio = pd.DataFrame(columns=df_cadastro_completo.columns)
            
            base_builder = BaseReportBuilder(self.config)
            base_report = base_builder.build(
                active_students=df_alunos_ativos_para_relatorio,
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
            
            calculator = KpiCalculatorPadrao(weekly_report, processed_data, self.config)
            report_with_kpis = calculator.calculate()
            
            log.info("Cálculo de KPI: Status de atingimento concluído.")

            log.info("Geração de Abas: Formatando relatórios de saída...")
            
            action_gen = ActionSheetGenerator(processed_data, self.config)
            summary_gen = SummarySheetGenerator(report_with_kpis, self.config)
            kpi_gen = KpiSheetGenerator(report_with_kpis)

            final_tabs = {}
            final_tabs.update(action_gen.generate())
            final_tabs.update(summary_gen.generate())
            final_tabs.update(kpi_gen.generate())
            
            log.info(f"Geração de Abas: {len(final_tabs)} abas criadas.")

            log.info("Escrita: Salvando arquivo Excel final...")
            output_file_path = self.data_writer.save_report_to_excel(
                report_tabs=final_tabs,
                base_filename="relatorio_presenca_stonelab"
            )
            
            log.info("Sucesso: Pipeline concluído.")
            log.info(f"Arquivo final salvo em: {output_file_path}")
            
            return output_file_path

        except Exception as e:
            log.error(f"Falha no Pipeline: {e}", exc_info=True)
            return ""