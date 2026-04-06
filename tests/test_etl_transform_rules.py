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


def test_finance_rows_without_counterparty_are_not_prefiltered() -> None:
    assert "is_financeiro_lancamentos" not in etl_run.POST_TRANSFORM_VALIDATORS


def test_finance_relations_are_not_mutated_or_dropped() -> None:
    assert "is_financeiro_lancamentos" not in etl_run.POST_TRANSFORM_MUTATORS


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


@pytest.mark.parametrize(
    ("table", "row", "transform"),
    [
        ("is_clientes", {"id": 123, "tipo": "fisica"}, lambda row: etl_run.transform_cliente(row)),
        ("is_financeiro_funcionarios", {"id": 123}, lambda row: etl_run.transform_row(row, "is_financeiro_funcionarios", etl_run.COLUMN_MAPPING["is_financeiro_funcionarios"])),
        ("is_produtos", {"id": 123}, lambda row: etl_run.transform_row(row, "is_produtos", etl_run.COLUMN_MAPPING["is_produtos"])),
        ("is_financeiro_lancamentos", {"id": 123, "tipo": 1}, lambda row: etl_run.transform_row(row, "is_financeiro_lancamentos", etl_run.COLUMN_MAPPING["is_financeiro_lancamentos"])),
        ("is_pedidos", {"id": 123}, lambda row: etl_run.transform_pedido(row, set())),
        ("is_pedidos_itens", {"id": 123}, lambda row: etl_run.transform_row(row, "is_pedidos_itens", etl_run.COLUMN_MAPPING["is_pedidos_itens"])),
        ("is_pedidos_pagamentos", {"id": 123}, lambda row: etl_run.transform_pagamento(row)),
        ("is_pedidos_historico", {"id": 123}, lambda row: etl_run.transform_row(row, "is_pedidos_historico", etl_run.COLUMN_MAPPING["is_pedidos_historico"])),
    ],
)
def test_erp_id_stays_numeric_while_primary_id_remains_uuid(table: str, row: dict, transform) -> None:
    transformed = transform(row)

    assert transformed is not None
    assert transformed["erp_id"] == 123
    assert transformed["id"] == etl_run.uuid5_for(table, 123)
