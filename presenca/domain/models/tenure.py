from datetime import datetime, date
from typing import List, Optional
from pydantic import BaseModel

class FrequencyChange(BaseModel):
    """Modela um evento de mudança de frequência na jornada de um aluno."""
    reference_date: date
    new_expected_frequency: int

class Record(BaseModel):
    """Modela uma única linha da planilha de entrada/saída (I/O Aluno)."""
    reference_date: date
    expected_frequency: Optional[int] = None

    def is_closing(self) -> bool:
        """Verifica se o registro representa o fim de uma jornada."""
        return self.expected_frequency is None

    def get_expected_frequency(self) -> int:
        """Retorna a frequência esperada, se não for um registro de fechamento."""
        if self.is_closing():
            raise ValueError("Cannot get frequency from a closing record.")
        return self.expected_frequency

class Tenure(BaseModel):
    """Modela a 'jornada' ou período de atividade de um participante."""
    beginning: date
    end: Optional[date] = None
    original_expected_frequency: int
    frequency_changes: List[FrequencyChange] = []

    def active_at_date(self, ref_date: date) -> bool:
        """Verifica se uma data está dentro do período de atividade."""
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()
        is_after_beginning = ref_date >= self.beginning
        is_before_end = self.end is None or ref_date <= self.end
        return is_after_beginning and is_before_end

    def get_expected_frequency(self, ref_date: date) -> int:
        """Retorna a frequência esperada correta para uma data específica."""
        relevant_changes = sorted(
            [fc for fc in self.frequency_changes if fc.reference_date <= ref_date],
            key=lambda fc: fc.reference_date,
            reverse=True,
        )
        if relevant_changes:
            return relevant_changes[0].new_expected_frequency
        return self.original_expected_frequency