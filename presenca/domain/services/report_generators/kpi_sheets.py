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
            log.warning("KpiSheets: DataFrame base (gpb) está vazio. Abas de KPI estarão vazias.")
            return {
                schema.ABA_REPORT_RAW: report_raw_output,
                schema.ABA_PIVOT_TOTAL: pd.DataFrame(),
                schema.ABA_PIVOT_ATINGIDOS: pd.DataFrame(),
                schema.ABA_PIVOT_PCT: pd.DataFrame(),
                schema.ABA_KPI_GERAL: pd.DataFrame()
            }

        pivots = self._generate_pivots(gpb)
        kpi_geral = self._generate_kpi_geral(gpb)
        
        final_tabs = {
            schema.ABA_REPORT_RAW: report_raw_output,
            schema.ABA_KPI_GERAL: kpi_geral
        }
        final_tabs.update(pivots)
        
        return final_tabs

    def _prepare_report_raw_output(self) -> pd.DataFrame:
        if self.report_kpi.empty:
            return pd.DataFrame()
            
        report_output = self.report_kpi.copy()
        
        if schema.COL_COORDINATOR in report_output.columns:
            def clean_coord_name(x):
                if hasattr(x, 'name'): 
                    return x.name
                s = str(x)
                if "Coordinator(name=" in s:
                    return s.replace(
                        "Coordinator(name='", "").replace("')", "").replace(
                            'Coordinator(name="', '').replace('")', '')
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
            log.warning("KpiSheets: report_kpi está vazio, pulando _calculate_base_metrics.")
            return pd.DataFrame()
            
        if schema.OUT_COL_SITUACAO not in self.report_kpi.columns:
            log.error(f"KpiSheets: Coluna '{schema.OUT_COL_SITUACAO}' não encontrada.")
            return pd.DataFrame()

        df = self.report_kpi[
            self.report_kpi[schema.OUT_COL_SITUACAO].str.strip() != schema.STATUS_JUSTIFICADO
        ].copy()
        
        if df.empty:
            log.warning("KpiSheets: Nenhum dado não-justificado para calcular métricas base.")
            return pd.DataFrame()

        df["Atingimento_Bin"] = df[schema.OUT_COL_SITUACAO].map(
            lambda x: 1 if x == schema.STATUS_ATINGIU else 0
        )
        
        df['coordinator_name'] = df[schema.COL_COORDINATOR].apply(
            lambda x: x.name if hasattr(x, 'name') else str(x).replace("Coordinator(name='", "").replace("')", "")
        )
        
        gpb = df.groupby(by=['coordinator_name', schema.COL_DATE]).agg(
            total_de_alunos=(schema.COL_ID_STONELAB, "count"),
            atingidos=("Atingimento_Bin", "sum")
        ).reset_index()
        
        gpb["pct_atingimento"] = 0.0
        mask = gpb["total_de_alunos"] > 0
        gpb.loc[mask, "pct_atingimento"] = (
            gpb.loc[mask, "atingidos"] / gpb.loc[mask, "total_de_alunos"]
        ).round(2)
        
        return gpb

    def _generate_pivots(self, gpb: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        
        pivots = {}
        pivot_fields = {
            "total_de_alunos": schema.ABA_PIVOT_TOTAL,
            "atingidos": schema.ABA_PIVOT_ATINGIDOS,
            "pct_atingimento": schema.ABA_PIVOT_PCT
        }
        
        for field_name, tab_name in pivot_fields.items():
            pivot = pd.pivot_table(
                gpb, 
                values=field_name, 
                index='coordinator_name', 
                columns=schema.COL_DATE, 
                aggfunc="sum"
            ).reset_index().fillna(0)
            
            pivot.columns = [
                col.strftime('%Y-%m-%d') if isinstance(col, (date, datetime)) else col 
                for col in pivot.columns
            ]
            
            pivot.rename(columns={'coordinator_name': schema.OUT_COL_COORDENADOR}, inplace=True)
            pivots[tab_name] = pivot
        
        return pivots

    def _generate_kpi_geral(self, gpb: pd.DataFrame) -> pd.DataFrame:
        
        kpi_geral = gpb.groupby(schema.COL_DATE).agg(
            total_de_alunos=("total_de_alunos", "sum"),
            atingidos=("atingidos", "sum")
        ).reset_index()
        
        kpi_geral["KPI_Presenca"] = 0.0
        mask = kpi_geral["total_de_alunos"] > 0
        kpi_geral.loc[mask, "KPI_Presenca"] = (
            kpi_geral.loc[mask, "atingidos"] / kpi_geral.loc[mask, "total_de_alunos"]
        ).round(2)
        
        return kpi_geral