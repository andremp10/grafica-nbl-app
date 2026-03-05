"""Unit tests for ETL transformation rules in etl.run."""

from __future__ import annotations

import json

import pytest

from etl import run as etl_run


def test_finance_tipo_mapping_legacy_to_target() -> None:
    overrides = etl_run.TABLE_TYPE_OVERRIDES["is_financeiro_lancamentos"]
    assert etl_run.transform_column("tipo", "tipo", 0, "is_financeiro_lancamentos", overrides) == 2
    assert etl_run.transform_column("tipo", "tipo", 1, "is_financeiro_lancamentos", overrides) == 1


def test_finance_tipo_invalid_is_fatal() -> None:
    overrides = etl_run.TABLE_TYPE_OVERRIDES["is_financeiro_lancamentos"]
    with pytest.raises(ValueError, match="tipo inv"):
        etl_run.transform_column("tipo", "tipo", 7, "is_financeiro_lancamentos", overrides)


@pytest.mark.parametrize(
    ("parcelas_raw", "expected_qtd"),
    [
        ("0", None),
        ("3x", 3),
        ("", None),
    ],
)
def test_parcelas_qtd_normalization(parcelas_raw: str, expected_qtd: int | None) -> None:
    row = {"id": 10, "parcelas": parcelas_raw}
    transformed = etl_run.transform_pagamento(row)
    assert transformed is not None
    assert transformed["parcelas_qtd"] == expected_qtd


def test_pf_pj_split_is_deterministic() -> None:
    fisica = {
        "id": 1,
        "tipo": "fisica",
        "nome": "Maria",
        "sobrenome": "Silva",
        "nascimento": "1990-01-01 00:00:00",
        "cpf": "000.000.000-00",
        "sexo": "Feminino",
    }
    juridica = {
        "id": 2,
        "tipo": "juridica",
        "razao_social": "Empresa Teste Ltda",
        "fantasia": "Empresa Teste",
        "ie": "123",
        "cnpj": "00.000.000/0001-00",
    }

    assert etl_run.transform_cliente_pf(fisica) is not None
    assert etl_run.transform_cliente_pj(fisica) is None

    assert etl_run.transform_cliente_pf(juridica) is None
    assert etl_run.transform_cliente_pj(juridica) is not None


def test_moderate_text_cleaning_rules() -> None:
    assert etl_run.clean_human_text("F&amp;A", "nome") == "F&A"
    assert etl_run.clean_human_text(r"D\\'VYMME", "nome") == "D'VYMME"
    assert etl_run.clean_human_text("********", "nome") is None
    assert etl_run.clean_human_text("  Maria   da   Silva  ", "nome") == "Maria da Silva"


def test_run_uses_json_mapping_as_source_of_truth() -> None:
    payload = json.loads(etl_run.MAPPING_PATH.read_text(encoding="utf-8"))
    assert etl_run.COLUMN_MAPPING == payload
    assert "is_financeiro_lancamentos" in etl_run.COLUMN_MAPPING
