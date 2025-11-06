import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from .models.tenure import Record, FrequencyChange, Tenure

class TenureFactory:
    """
    Classe especializada em construir uma lista de Tenures
    para cada participante.
    """
    def create_tenures_from_df(self, df: pd.DataFrame) -> Dict[str, List[Tenure]]:
        """
        Orquestra o processo de criação das jornadas para todos os participantes.
        """
        df_io = self._prepare_io_dataframe(df)
        all_tenures = {}
        for pid, group in df_io.groupby('id'):
            if not pid or pd.isna(pid):
                continue
            records = self._create_records_from_group(group)            
            all_tenures[str(pid)] = self._build_tenures_from_records(records)   
        return all_tenures

    def _prepare_io_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e prepara o DataFrame da planilha I/O Aluno."""
        df_io = df.copy()
        df_io.columns = df_io.columns.str.strip()
        
        df_io['ref_date'] = pd.to_datetime(
            df_io["Quando a data de referência?"],
            format="%d/%m/%Y",
            errors='coerce'
        ).dt.date
        
        id_col_name = 'id_stonelab'
        if id_col_name not in df_io.columns:
            if 'ID_Stonelab' in df_io.columns:
                df_io.rename(columns={'ID_Stonelab': id_col_name}, inplace=True)
            else:
                raise KeyError(
                    "Coluna de ID do Stonelab não encontrada na planilha de I/O."
                )
                               
        df_io['id'] = df_io[id_col_name]
        df_io['exp_freq'] = pd.to_numeric(df_io["Frequência esperada"], errors='coerce')
        return df_io

    def _create_records_from_group(self, group: pd.DataFrame) -> List[Record]:
        """Converte as linhas de um DataFrame de um único aluno em objetos Record."""
        records = []
        for row in group.sort_values('ref_date').itertuples():
            freq = None if pd.isna(row.exp_freq) else int(row.exp_freq)
            if pd.notna(row.ref_date):
                records.append(Record(reference_date=row.ref_date, expected_frequency=freq))
        return records

    def _build_tenures_from_records(self, records: List[Record]) -> List[Tenure]:
        """Constrói a linha do tempo (lista de Tenures) a partir dos Records."""
        tenures, current_tenure = [], None
        for record in records:
            if record.is_closing():
                if current_tenure:
                    current_tenure.end = record.reference_date
                    tenures.append(current_tenure)
                    current_tenure = None
            elif current_tenure:
                change = FrequencyChange(
                    reference_date=record.reference_date,
                    new_expected_frequency=record.get_expected_frequency()
                )
                current_tenure.frequency_changes.append(change)
            else:
                current_tenure = Tenure(
                    beginning=record.reference_date,
                    original_expected_frequency=record.get_expected_frequency()
                )
        if current_tenure:
            tenures.append(current_tenure)
        return tenures