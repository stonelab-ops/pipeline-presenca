import pandas as pd
import logging
import numpy as np
from ...utils import schema
from .kpi_calculator_base import KpiCalculatorBase

log = logging.getLogger(__name__)

class KpiCalculatorPadrao(KpiCalculatorBase):

    def _initialize(self):
        log.info("Calculadora KPI (Padrão): Inicializada.")

    def calculate(self) -> pd.DataFrame:
        report_kpi = self._filter_report_for_month()
        report_kpi = self._apply_precise_frequency(report_kpi)
        
        if report_kpi.empty:
            log.warning("Calculadora KPI (Padrão): Relatório base está vazio, pulando cálculo.")
            return report_kpi

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
            schema.STATUS_ATINGIU,
            schema.STATUS_JUSTIFICADO,
            schema.STATUS_JUSTIFICADO,
            schema.STATUS_ATINGIU
        ]

        report_kpi[schema.OUT_COL_SITUACAO] = np.select(
            conditions, 
            choices, 
            default=schema.STATUS_NAO_ATINGIU
        )
        
        report_kpi.drop(columns=['dias_faltados'], inplace=True)
        
        return report_kpi

    def _filter_report_for_month(self) -> pd.DataFrame:
        if self.report.empty:
            return self.report
            
        start_date = pd.to_datetime(self.config.DATA_INICIO_GERAL)
        end_date = pd.to_datetime(self.config.DATA_FIM_GERAL)
        
        return self.report[
            (pd.to_datetime(self.report[schema.COL_DATE]) >= start_date) &
            (pd.to_datetime(self.report[schema.COL_DATE]) <= end_date)
        ].copy()

    def _apply_precise_frequency(self, report_kpi: pd.DataFrame) -> pd.DataFrame:
        start_date = pd.to_datetime(self.config.DATA_INICIO_GERAL).date()
        end_date = pd.to_datetime(self.config.DATA_FIM_GERAL).date()
        
        df_registros_final = self.data['registros_final']

        if df_registros_final.empty:
            log.warning("Calculadora KPI (Padrão): 'registros_final' vazio. Frequência observada será 0.")
            if not report_kpi.empty:
                report_kpi['observed_frequency'] = 0
            return report_kpi

        presencas_do_mes = df_registros_final[
            (pd.to_datetime(df_registros_final[schema.COL_XML_DATE]).dt.date >= start_date) & 
            (pd.to_datetime(df_registros_final[schema.COL_XML_DATE]).dt.date <= end_date)
        ].copy() 

        if not presencas_do_mes.empty:
            iso_dates = pd.to_datetime(presencas_do_mes[schema.COL_XML_DATE]).dt.isocalendar()
            presencas_do_mes['year'] = iso_dates.year
            presencas_do_mes['week'] = iso_dates.week
        else:
            presencas_do_mes['year'] = []
            presencas_do_mes['week'] = []

        freq_obs_precisa = presencas_do_mes.groupby(
            [schema.COL_ID_STONELAB, 'year', 'week']
        ).size().reset_index(name='observed_frequency_precisa')
        
        if 'observed_frequency' in report_kpi.columns:
            report_kpi = report_kpi.drop(columns=['observed_frequency'])
            
        report_kpi = pd.merge(
            report_kpi, freq_obs_precisa, 
            on=[schema.COL_ID_STONELAB, 'year', 'week'], how='left'
        )
        report_kpi.rename(
            columns={'observed_frequency_precisa': 'observed_frequency'}, 
            inplace=True
        )
        report_kpi['observed_frequency'] = report_kpi[
            'observed_frequency'
        ].fillna(0).astype(int)
        return report_kpi