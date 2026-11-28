"""`vectordbbench samyamagraph ...` (#1984).

Copied into a VectorDBBench checkout as
`vectordb_bench/backend/clients/samyama_graph/cli.py`; see ../README.md.
"""

from typing import Annotated, Unpack

import click
from pydantic import SecretStr

from vectordb_bench.backend.clients import DB

from ....cli.cli import (
    CommonTypedDict,
    HNSWFlavor1,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)


class SamyamaGraphTypedDict(CommonTypedDict, HNSWFlavor1):
    url: Annotated[
        str,
        click.option("--url", type=str, default="http://127.0.0.1:8080", show_default=True, help="HTTP API"),
    ]
    token: Annotated[
        str | None,
        click.option("--token", type=str, default=None, help="Bearer token, when the server requires one"),
    ]
    graph: Annotated[
        str,
        click.option("--graph", type=str, default="default", show_default=True, help="Graph (tenant)"),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(SamyamaGraphTypedDict)
def SamyamaGraph(**parameters: Unpack[SamyamaGraphTypedDict]):
    from .config import SamyamaGraphConfig, SamyamaGraphIndexConfig

    run(
        db=DB.SamyamaGraph,
        db_config=SamyamaGraphConfig(
            db_label=parameters["db_label"],
            url=parameters["url"],
            token=SecretStr(parameters["token"]) if parameters["token"] else None,
            graph=parameters["graph"],
        ),
        db_case_config=SamyamaGraphIndexConfig(
            m=parameters["m"] or 16,
            ef_construction=parameters["ef_construction"] or 200,
            ef_search=parameters["ef_search"] or 128,
        ),
        **parameters,
    )
