import pandas as pd
from datetime import date
import unicodedata 

class KpiSheetGenerator:
    def __init__(self, report_kpi: pd.DataFrame):
        self.report_kpi = report_kpi
        self.internal_status_col = 'Situação de Atingimento'

    def generate(self) -> dict:
        tabs = {}
        tabs['report_raw'] = self._create_report_raw()
        
        gpb = self._calculate_base_metrics()
        tabs.update(self._create_pivot_tabs(gpb))
        
        tabs['kpi_geral_presenca'] = self._create_kpi_geral_tab(gpb)
        return tabs

    def _create_report_raw(self) -> pd.DataFrame:
        translation = {
            "name": "Nome", "function": "Função", "coordinator": "Coordenador",
            "date": "Semana", "observed_frequency": "Freq Obs",
            "expected_frequency": "Freq Esp", "workdays": "Dias Úteis",
            "justified_days": "Falt Just", "vacation_days": "Dias de Férias",
            self.internal_status_col: "Situação de Atingimento"
        }
        renamed = self.report_kpi.rename(columns=translation)
            
        cols = [c for c in translation.values() if c in renamed.columns]
        return renamed[cols]


    def _calculate_base_metrics(self) -> pd.DataFrame:
        

        report = self.report_kpi[
            self.report_kpi['Situação de Atingimento'].str.strip() != "Semana Justificada"
        ].copy()
    
        report["Atingimento_Bin"] = report["Situação de Atingimento"].map(
            lambda x: 1 if x.strip() == "Atingiu" else 0 
        )
        
        gpb = report.groupby(by=["coordinator", "date"]).agg(
            total_de_alunos=("id_stonelab", "count"),
            atingidos=("Atingimento_Bin", "sum")
        ).reset_index()
        
        gpb["pct_atingimento"] = 0.0
        mask = gpb["total_de_alunos"] > 0
        gpb.loc[mask, "pct_atingimento"] = (
            gpb.loc[mask, "atingidos"] / gpb.loc[mask, "total_de_alunos"]
        ).round(2)
        
        return gpb

    def _create_pivot_tabs(self, gpb: pd.DataFrame) -> dict:
        pivots = {}
        for field in ["total_de_alunos", "atingidos", "pct_atingimento"]:
            pivot = pd.pivot_table(
                gpb, values=field, index="coordinator",
                columns="date", aggfunc="sum"
            ).reset_index().fillna(0)
            pivot.columns = [
                c.strftime('%Y-%m-%d') if isinstance(c, (date, pd.Timestamp)) else c
                for c in pivot.columns
            ]
            pivots[f'{field}_pivot'] = pivot
        return pivots

    def _create_kpi_geral_tab(self, gpb: pd.DataFrame) -> pd.DataFrame:
        kpi_geral = gpb.groupby("date").agg(
            total_de_alunos=("total_de_alunos", "sum"),
            atingidos=("atingidos", "sum")
        ).reset_index()
        kpi_geral["KPI_Presenca"] = 0.0
        mask = kpi_geral["total_de_alunos"] > 0
        
        division_result = (kpi_geral.loc[mask, "atingidos"] / kpi_geral.loc[mask, "total_de_alunos"])
        kpi_geral.loc[mask, "KPI_Presenca"] = pd.to_numeric(division_result).round(2)
        
        return kpi_geral