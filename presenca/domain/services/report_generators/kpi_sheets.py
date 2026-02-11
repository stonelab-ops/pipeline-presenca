import pandas as pd
import logging
from typing import Dict, Any
from datetime import date, datetime
import schema
from ...models.coordinator import Coordinator

log = logging.getLogger(__name__)

class KpiSheetGenerator:
    
    def __init__(self, report_kpi: pd.DataFrame):
        self.report_kpi = report_kpi

    def generate(self) -> Dict[str, pd.DataFrame]:
        report_raw_output = self._prepare_report_raw_output()
        
        gpb = self._calculate_base_metrics()
        
        if gpb.empty:
            log.warning("KpiSheets: DataFrame base (gpb) está vazio. Gerando apenas RAW.")
            return {
                schema.ABA_REPORT_RAW: report_raw_output,
                schema.ABA_KPI_GERAL: pd.DataFrame()
            }

        kpi_geral = self._generate_kpi_geral(gpb)
        
        return {
            schema.ABA_REPORT_RAW: report_raw_output,
            schema.ABA_KPI_GERAL: kpi_geral
        }

    def _prepare_report_raw_output(self) -> pd.DataFrame:
        if self.report_kpi.empty:
            return pd.DataFrame()
            
        report_output = self.report_kpi.copy()
        
        if schema.COL_COORDINATOR in report_output.columns:
            def clean_coord_name(x):
                if hasattr(x, 'name'): return x.name
                s = str(x)
                if "Coordinator(name=" in s:
                    return s.replace("Coordinator(name='", "").replace("')", "").replace('Coordinator(name="', '').replace('")', '')
                return s

            report_output[schema.COL_COORDINATOR] = report_output[schema.COL_COORDINATOR].apply(clean_coord_name)

        column_map = {
            schema.COL_ID_STONELAB: schema.COL_ID_STONELAB,
            schema.COL_NAME: schema.OUT_COL_NOME,
            schema.COL_FUNCTION: schema.OUT_COL_FUNCAO,
            schema.COL_COORDINATOR: schema.OUT_COL_COORDENADOR,
            schema.COL_DATE: schema.OUT_COL_SEMANA,
            "observed_frequency": schema.OUT_COL_FREQ_OBS,
            "expected_frequency": schema.OUT_COL_FREQ_ESP,
            "meta_dinamica": "Meta Dinâmica (60%)",
            "workdays": schema.OUT_COL_DIAS_UTEIS,
            "justified_days": schema.OUT_COL_FALTAS_JUST,
            "vacation_days": schema.OUT_COL_DIAS_FERIAS,
            schema.OUT_COL_SITUACAO: schema.OUT_COL_SITUACAO
        }
        
        cols_to_keep = [col for col in column_map.keys() if col in report_output.columns]
        report_output = report_output[cols_to_keep]
        report_output.rename(columns=column_map, inplace=True)
        
        return report_output

    def _calculate_base_metrics(self) -> pd.DataFrame:
        if self.report_kpi.empty:
            return pd.DataFrame()
            
        if schema.OUT_COL_SITUACAO not in self.report_kpi.columns:
            return pd.DataFrame()

        df = self.report_kpi[
            self.report_kpi[schema.OUT_COL_SITUACAO].str.strip() != schema.STATUS_JUSTIFICADO
        ].copy()
        
        if df.empty:
            return pd.DataFrame()

        df["Atingimento_Bin"] = df[schema.OUT_COL_SITUACAO].map(
            lambda x: 1 if x == schema.STATUS_ATINGIU else 0
        )
        
        gpb = df.groupby(by=[schema.COL_DATE]).agg(
            total_de_alunos=(schema.COL_ID_STONELAB, "count"),
            atingidos=("Atingimento_Bin", "sum")
        ).reset_index()
        
        return gpb

    def _generate_kpi_geral(self, gpb: pd.DataFrame) -> pd.DataFrame:
        kpi_geral = gpb.copy()
        kpi_geral["KPI_Presenca"] = 0.0
        
        mask = kpi_geral["total_de_alunos"] > 0
        kpi_geral.loc[mask, "KPI_Presenca"] = (
            kpi_geral.loc[mask, "atingidos"] / kpi_geral.loc[mask, "total_de_alunos"]
        ).round(2)
        
        return kpi_geral