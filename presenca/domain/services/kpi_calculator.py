import pandas as pd
import logging
import numpy as np

log = logging.getLogger(__name__)

class KpiCalculator:

    def __init__(self, report: pd.DataFrame, data_frames: dict, config: dict):
        self.report = report
        self.data = data_frames
        self.config = config
        log.info("Serviço de Cálculo de KPI inicializado.")

    def calculate(self) -> pd.DataFrame:
        report_kpi = self._filter_report_for_month()
        report_kpi = self._apply_precise_frequency(report_kpi)
        
        if report_kpi.empty:
            log.warning("Relatório de KPI está vazio, pulando cálculo de 'Situação de Atingimento'.")
            return report_kpi

        # --- INÍCIO DA LÓGICA VETORIZADA ---
        
        report_kpi['dias_faltados'] = report_kpi['expected_frequency'] - report_kpi['observed_frequency']
        
        ratio_obs = (
            report_kpi['observed_frequency'] / report_kpi['workdays']
        ).replace([np.inf, -np.inf], 0).fillna(0)
        
        ratio_exp = (report_kpi['expected_frequency'] / 5).fillna(0)

        conditions = [
            (report_kpi['expected_frequency'] == 0),
            (report_kpi['workdays'] <= 1),
            (report_kpi['dias_faltados'] > 0) & (report_kpi['dias_faltados'] <= report_kpi['justified_days']),
            (ratio_obs >= ratio_exp) & (report_kpi['workdays'] > 0)
        ]
        
        choices = [
            "Atingiu",
            "Semana Justificada",
            "Semana Justificada",
            "Atingiu"
        ]

        report_kpi['Situação de Atingimento'] = np.select(
            conditions, 
            choices, 
            default="Não Atingiu"
        )
        
        report_kpi.drop(columns=['dias_faltados'], inplace=True)
        
        # --- FIM DA LÓGICA VETORIZADA ---
        
        return report_kpi

    def _filter_report_for_month(self) -> pd.DataFrame:
        if self.report.empty:
            return self.report
            
        start_date = pd.to_datetime(self.config.DATA_INICIO_GERAL)
        end_date = pd.to_datetime(self.config.DATA_FIM_GERAL)
        
        return self.report[
            (pd.to_datetime(self.report["date"]) >= start_date) &
            (pd.to_datetime(self.report["date"]) <= end_date)
        ].copy()

    def _apply_precise_frequency(self, report_kpi: pd.DataFrame) -> pd.DataFrame:
        start_date = pd.to_datetime(self.config.DATA_INICIO_GERAL).date()
        end_date = pd.to_datetime(self.config.DATA_FIM_GERAL).date()
        
        df_registros_final = self.data['registros_final']

        if df_registros_final.empty:
            log.warning("DataFrame 'registros_final' está vazio. Frequência observada será 0.")
            if not report_kpi.empty:
                report_kpi['observed_frequency'] = 0
            return report_kpi

        presencas_do_mes = df_registros_final[
            (pd.to_datetime(df_registros_final['Date']).dt.date >= start_date) & 
            (pd.to_datetime(df_registros_final['Date']).dt.date <= end_date)
        ].copy() 

        if not presencas_do_mes.empty:
            iso_dates = pd.to_datetime(presencas_do_mes['Date']).dt.isocalendar()
            presencas_do_mes['year'] = iso_dates.year
            presencas_do_mes['week'] = iso_dates.week
        else:
            presencas_do_mes['year'] = []
            presencas_do_mes['week'] = []

        freq_obs_precisa = presencas_do_mes.groupby(
            ['id_stonelab', 'year', 'week']
        ).size().reset_index(name='observed_frequency_precisa')
        
        if 'observed_frequency' in report_kpi.columns:
            report_kpi = report_kpi.drop(columns=['observed_frequency'])
            
        report_kpi = pd.merge(
            report_kpi, freq_obs_precisa, 
            on=['id_stonelab', 'year', 'week'], how='left'
        )
        report_kpi.rename(
            columns={'observed_frequency_precisa': 'observed_frequency'}, 
            inplace=True
        )
        report_kpi['observed_frequency'] = report_kpi[
            'observed_frequency'
        ].fillna(0).astype(int)
        return report_kpi