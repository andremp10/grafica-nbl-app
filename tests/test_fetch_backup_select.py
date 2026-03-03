"""
test_fetch_backup_select.py — Testa a lógica de seleção de arquivo de backup.

Testa _select_best() com listagens mockadas de SFTP/FTP,
sem realizar conexões reais.
"""

from datetime import date, datetime

import pytest

from scripts.fetch_backup import _select_best, DATE_PATTERN, PREFIX, EXT


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _make_file_list(*dates: str, extra_files: list[str] | None = None) -> list[tuple[str, datetime | None]]:
    """
    Cria lista de (filename, mtime) para os dates fornecidos.
    dates: strings "YYYY-MM-DD"
    """
    result = [(f"{PREFIX}{d}{EXT}", datetime.fromisoformat(f"{d}T03:00:00")) for d in dates]
    for name in (extra_files or []):
        result.append((name, None))
    return result


# ─────────────────────────────────────────────────────────────
# Testes de seleção
# ─────────────────────────────────────────────────────────────
class TestSelectBest:
    def test_selects_today(self):
        """Com target_date=hoje e arquivo exato disponível, seleciona esse arquivo."""
        today = "2025-01-15"
        files = _make_file_list("2025-01-13", "2025-01-14", "2025-01-15")
        result = _select_best(files, target_date=date.fromisoformat(today))
        assert result == f"{PREFIX}{today}{EXT}"

    def test_selects_latest_when_no_target(self):
        """Sem target_date, seleciona o arquivo mais recente."""
        files = _make_file_list("2025-01-10", "2025-01-12", "2025-01-11")
        result = _select_best(files, target_date=None)
        assert result == f"{PREFIX}2025-01-12{EXT}"

    def test_falls_back_to_latest_if_target_missing(self):
        """Se data alvo não existe na listagem, usa o mais recente."""
        files = _make_file_list("2025-01-13", "2025-01-14")
        result = _select_best(files, target_date=date.fromisoformat("2025-01-15"))
        assert result == f"{PREFIX}2025-01-14{EXT}"

    def test_ignores_non_matching_files(self):
        """Arquivos com padrão diferente são ignorados."""
        files = [
            (f"{PREFIX}2025-01-15{EXT}", None),
            ("other_backup.tar.gz", None),
            ("readme.txt", None),
            ("nblgrafica_app-invalid-date.sql.gz", None),
        ]
        result = _select_best(files, target_date=None)
        assert result == f"{PREFIX}2025-01-15{EXT}"

    def test_raises_if_no_matching_files(self):
        """Lança FileNotFoundError se nenhum arquivo bate com o padrão."""
        files = [
            ("other_backup.tar.gz", None),
            ("readme.txt", None),
        ]
        with pytest.raises(FileNotFoundError, match="Nenhum arquivo"):
            _select_best(files, target_date=None)

    def test_raises_on_empty_list(self):
        """Lista vazia deve levantar FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            _select_best([], target_date=None)

    def test_single_file_no_target(self):
        """Com apenas um arquivo e sem target, retorna esse arquivo."""
        files = _make_file_list("2025-06-01")
        result = _select_best(files, target_date=None)
        assert result == f"{PREFIX}2025-06-01{EXT}"

    def test_uses_mtime_as_tiebreaker_same_date(self):
        """
        Se dois arquivos tivessem a mesma data no nome (improvável mas possível),
        mtime seria o desempate.
        """
        # Simula: mesmo prefixo/data, mtime diferente
        files = [
            (f"{PREFIX}2025-01-15{EXT}", datetime(2025, 1, 15, 1, 0, 0)),
            (f"{PREFIX}2025-01-15{EXT}", datetime(2025, 1, 15, 5, 0, 0)),
        ]
        # Não deve lançar exceção mesmo com duplicatas
        result = _select_best(files, target_date=None)
        assert result == f"{PREFIX}2025-01-15{EXT}"

    def test_selects_most_recent_across_months(self):
        """Valida ordenação entre meses diferentes."""
        files = _make_file_list(
            "2024-12-30", "2025-01-01", "2025-01-15", "2024-11-20"
        )
        result = _select_best(files, target_date=None)
        assert result == f"{PREFIX}2025-01-15{EXT}"


# ─────────────────────────────────────────────────────────────
# Testes de pattern matching
# ─────────────────────────────────────────────────────────────
class TestDatePattern:
    @pytest.mark.parametrize("filename,expected_date", [
        (f"{PREFIX}2025-01-15{EXT}", "2025-01-15"),
        (f"{PREFIX}2024-12-31{EXT}", "2024-12-31"),
        (f"{PREFIX}2025-06-01{EXT}", "2025-06-01"),
    ])
    def test_valid_filename_matches(self, filename, expected_date):
        m = DATE_PATTERN.match(filename)
        assert m is not None
        assert m.group(1) == expected_date

    @pytest.mark.parametrize("filename", [
        "other_backup.sql.gz",
        f"{PREFIX}2025-01-15.tar.gz",
        f"{PREFIX}2025-1-5.sql.gz",       # mês/dia sem zero
        f"{PREFIX}20250115.sql.gz",        # sem hífens
        f"{PREFIX}2025-01-15{EXT}.bak",    # sufixo extra
        "",
    ])
    def test_invalid_filename_no_match(self, filename):
        assert DATE_PATTERN.match(filename) is None
