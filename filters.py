from datetime import datetime
from inspect import currentframe
from json import dumps
from logging import Logger, config, getLogger
from numpy import ndarray
from pandas import DataFrame, Series, merge, isna
from typing import Any, Callable, Literal
from ParksGIS import (
    LayerDomainNames,
    LayerEdits,
    LayerQuery,
    LayerServerGen,
    Server,
)

__logger: Logger = getLogger("[ filters ]")


# Utility Functions
def exception_handler(e: Exception) -> None:
    # Get the current frame, then the previous frame (the caller)
    frame = currentframe().f_back
    # Get the name of the function from the frame
    raise Exception(
        {
            "function": frame.f_code.co_name,
            "inner": e,
        }
    )


def epoch_to_local_datetime(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch / 1000)


def to_json(obj: Any) -> str:
    if isinstance(obj, DataFrame):
        return obj.to_json(orient="records", date_format="iso")
    else:
        return dumps(obj)


def join(
    items: Series | list,
    withQuotations: bool = False,
) -> str:
    if isinstance(items, Series):
        items = list(filter_Nones(items))

    if withQuotations:
        joined = "','".join(str(i) for i in items)
        return joined if len(joined) == 0 else f"'{joined}'"
    else:
        return ",".join(str(i) for i in items)


def filter_Nones(
    data: DataFrame | Series,
    key: str | None = None,
) -> DataFrame | Series:
    if isinstance(data, DataFrame):
        if key is None:
            raise ValueError("Key required for DataFrame")
        return data[data[key].notna()]
    else:
        return data[data.notna()]


def update_df(
    destination: DataFrame,
    source: DataFrame,
    key: str,
    map: dict[str, str],
) -> DataFrame:
    unhasable_key = isinstance(
        source.at[0, key],
        list,
    ) or isinstance(
        source.at[0, key],
        ndarray,
    )

    dict = {
        str(row[key]) if unhasable_key else row[key]: row
        for _, row in source.iterrows()
    }

    result = DataFrame(columns=destination.columns)

    for _, dest_row in destination.iterrows():
        result_row = dest_row.copy()
        hashable_key = str(dest_row[key]) if unhasable_key else dest_row[key]

        if hashable_key in dict:
            src_row = dict[hashable_key]
            for dest_col, src_col in map.items():
                result_row[dest_col] = src_row[src_col]
        result.loc[len(result)] = result_row

    return result


def pipeline(
    context: dict[str, Any],
    *funcs: Callable[
        [dict[str, Any]],
        dict[str, Any] | None,
    ],
) -> dict[str, Any]:
    __logger = getLogger("[ pipeline ]")

    if not funcs:
        raise ValueError("At least one function must be provided.")

    if "log" in context:
        __logger.info(context["log"])

    context["output"] = {}

    result = context
    for _, func in enumerate(funcs):
        if result is None:
            __logger.debug(f"**Pipeline ended before {func.__name__}**")
            break

        if not isinstance(func, Callable):
            raise TypeError(
                f"Expected a callable for '{func if isinstance(func, str) else func.__name__ or func}'"
            )

        __logger.debug(f"Executing: {func.__name__}")
        result = func(result)

    return context


def get_deltas(context: dict) -> DataFrame | dict[str, DataFrame]:
    return context["deltas"][context["layer_id"]]


def set_deltas(
    context: dict,
    data: DataFrame | dict[str, DataFrame],
    layer_id: int | None = None,
) -> None:
    if layer_id is not None:
        context["layer_id"] = layer_id

    if "layer_id" not in context:
        raise Exception("layer_id is required!")

    if "edits" not in context:
        context["deltas"] = {}

    context["deltas"][context["layer_id"]] = data


def configure_rotating_logger(
    filename: str,
    directory: str | None = None,
    level: str = "INFO",
) -> None:
    from os import makedirs, path

    dir_path = path.join("home", directory or "")
    makedirs(dir_path, exist_ok=True)

    config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "detailed": {
                    "format": "%(asctime)s - %(levelname)s : %(name)s - %(message)s",
                }
            },
            "handlers": {
                "timed_rotating_file": {
                    "class": "logging.handlers.TimedRotatingFileHandler",
                    "level": level,
                    "formatter": "detailed",
                    "filename": path.join(dir_path, filename),
                    "when": "midnight",  # Rotate logs at midnight
                    "interval": 1,  # Rotate every day
                    "encoding": "utf-8",
                },
                "console": {
                    "class": "logging.StreamHandler",
                    "level": level,
                    "formatter": "detailed",
                },
            },
            "root": {
                "level": level,
                "handlers": ["timed_rotating_file", "console"],
            },
        }
    )

    # Enable http debug output
    import http.client

    http.client.HTTPConnection.debuglevel = 2 if level == "DEBUG" else 0


def set_proxy_variables(proxy: str) -> None:
    import os

    os.environ["HTTP_PROXY"] = proxy
    os.environ["HTTPS_PROXY"] = proxy
    # bypass proxy on parks domains
    os.environ["NO_PROXY"] = ".parks.nycnet"


#######################################################################################


# Common
def apply_edits(context: dict) -> dict | None:
    if context["deltas"] is not None and 0 < len(context["deltas"]):

        layer_edits: list[LayerEdits] = []
        for id, edits in context["deltas"].items():
            __logger.debug(f"LayerEdits: {id}, {edits}")
            layer_edits.append(
                LayerEdits(
                    id,
                    adds=edits.get("adds", None),
                    updates=edits.get("updates", None),
                )
            )

        try:
            result = context["repo"].apply_edits(layer_edits)
            __logger.debug(f"Edits Result: {to_json(result)}")

        except Exception as e:
            exception_handler(e)

    return context


def extract_changes(
    server: Server,
    layer_id: int,
    server_gen: int,
    out_fields: list[str],
    where: str = "",
) -> dict[str, Any]:
    try:
        changes = server.extract_changes(
            [
                LayerServerGen(
                    layer_id,
                    int(server_gen),
                )
            ]
        )

        result = {
            "changes": None,
            "server_gen": changes["layerServerGens"][0]["serverGen"],
        }

        object_ids: list[Any] = [
            *changes["edits"][0]["objectIds"]["adds"],
            *changes["edits"][0]["objectIds"]["updates"],
        ]
        if 0 == len(object_ids):
            return result

        agg_where = f"OBJECTID IN ({join(object_ids)})" + (
            "" if where == "" else f" AND {where}"
        )

        result["changes"] = server.query(
            [
                LayerQuery(
                    layer_id,
                    out_fields,
                    agg_where,
                )
            ]
        )[layer_id]
        return result

    except Exception as e:
        raise Exception(f"extract_changes: {e}")


def _seperate_changes(changes: DataFrame) -> dict[str, DataFrame]:
    adds = DataFrame(columns=changes.columns)
    updates = DataFrame(columns=changes.columns)
    for _, item in changes.iterrows():
        if isna(item["objectId"]):
            adds.loc[len(adds)] = item
        else:
            updates.loc[len(updates)] = item

    deltas: dict[str, DataFrame] = {}
    __logger.debug(f"Adds: {len(adds)}")
    if 0 < len(adds):
        deltas["adds"] = adds.drop(columns=["objectId"])
    __logger.debug(f"Updates: {len(updates)}")
    if 0 < len(updates):
        deltas["updates"] = updates

    return deltas


def query_server_gens(context: dict) -> dict:
    try:
        layer_id = 3
        server_gens = context["repo"].query(
            [LayerQuery(layer_id, ["*"])],
        )[layer_id]

    except Exception as e:
        exception_handler(e)

    for i, col in enumerate(server_gens.columns):
        if col != "OBJECTID":
            server_gens[col] = server_gens[col].astype("int64")
            server_gens.iloc[0, i] = server_gens.iloc[0, i] * 1000

    context["server_gens"] = server_gens
    return context


def apply_server_gens_edits(context: dict) -> dict | None:
    layer_id = 3
    server_gens = context["server_gens"].copy()

    for i, col in enumerate(server_gens.columns):
        if col != "OBJECTID":
            server_gens.iloc[0, i] = round(server_gens.iloc[0, i] / 1000)
        server_gens[col] = server_gens[col].astype("int32")

    edits = [LayerEdits(layer_id, updates=server_gens)]

    try:
        context["server_gens_repo"].apply_edits(edits)
        __logger.debug("Server Gens saved")

    except Exception as e:
        exception_handler(e)

    return context


def query_domains(context: dict) -> dict | None:
    try:
        context["domainValues"] = context["repo"].query_domains(
            [
                LayerDomainNames(
                    context["layerId"],
                    context["domainNames"],
                )
            ]
        )
        __logger.debug(f"Domain Values count: {len(context)}")
    except Exception as e:
        exception_handler(e)

    return context


def post_domains(context: dict) -> dict | None:
    domain_values = [
        {
            "domainName": domain["name"],
            "code": str(value["code"]),
            "value": value["name"],
        }
        for domain in context["domainValues"]
        for value in domain["codedValues"]
    ]

    try:
        response = context["service"].post_domain_values(domain_values)

    except Exception as e:
        exception_handler(e)

    context["output"]["post_domains"] = f"Domain Values result: {response}"
    return context


#######################################################################################


# Domain
def get_contract_edits(context: dict) -> dict | None:
    layer_id = 1
    from_date_time = epoch_to_local_datetime(context["server_gens"].at[0, "Contract"])

    try:
        response = context["service"].get_contracts(from_date_time)
        __logger.debug(f"Contracts Recieved: {len(response)} \n{to_json(response)}")

    except Exception as e:
        exception_handler(e)

    if 0 == len(response):
        context["output"]["contract_get_edits"] = "No Contract Changes."
        return None

    deltas = _seperate_changes(response)
    set_deltas(context, deltas, layer_id)

    return context


def query_contract_ids(context: dict) -> dict | None:
    layer_id = 1

    try:
        contracts = context["server_gens_repo"].query(
            [
                LayerQuery(
                    layer_id,
                    ["ContractName"],
                )
            ]
        )[layer_id]

    except Exception as e:
        exception_handler(e)

    if contracts.empty:
        context["output"]["query_contract_ids"] = "No Contracts found."
        return None

    context["contract_ids"] = (
        [] if contracts.empty else contracts["ContractName"].tolist()
    )
    return context


def contract_associated_work_order_extract_changes(context: dict) -> dict | None:
    layer_id = 0

    try:
        result = extract_changes(
            context["repo"],
            layer_id,
            context["server_gens"].at[0, "WorkOrder"],
            [
                # "FundingSource", Not found
                "ActualFinishDate",
                "Comments",
                "Contract",
                "LocationDetails",
                "Project",
                "PROJSTARTDATE",
                "RecommendedSpecies",
                "Status",
                "Type",
                "WOEntity",
                "CancelDate",
                "CancelReason",
                "CancelByERN",
                "CancelByName",
                "ClosedDate",
                "ClosedByERN",
                "ClosedByName",
                "ClosedBySystem",
                "CreatedDate",
                "CreatedBYERN",
                "CreatedByName",
                "UpdatedDate",
                "UpdatedByERN",
                "UpdatedByName",
                "GlobalID",
                "InspectionGlobalID",
                "OBJECTID",
            ],
            f"Contract in ({join(context['contract_ids'])})",
        )
        __logger.debug(
            f"Work Orders Extracted: {0 if result['changes'].empty else len(result['changes'])}"
        )

    except Exception as e:
        exception_handler(e)

    if result["changes"].empty:
        context["output"]["work_order_extract_changes"] = "No Work Order changes."
        return None

    set_deltas(context, result["changes"], layer_id)
    context["server_gens"].at[0, "WorkOrder"] = result["server_gen"]
    return context


def query_work_order_associated_planting_space_globalid(context: dict) -> dict | None:
    layer_id = 4
    key = "InspectionGlobalID"
    edits = DataFrame(get_deltas(context))

    try:
        inspections = (
            context["repo"]
            .query(
                [
                    LayerQuery(
                        layer_id,
                        [
                            "GlobalID",
                            "PlantingSpaceGlobalID",
                        ],
                        f"GlobalID IN ({join(edits[key], True)})",
                    )
                ]
            )[layer_id]
            .drop(columns=["OBJECTID"])
            .rename(columns={"GlobalID": key})
        )
        __logger.debug(f"Planting Space Ids found: {len(inspections)}")

    except Exception as e:
        exception_handler(e)

    edits = merge(edits, inspections, on=key, how="left")
    edits.drop(columns=["InspectionGlobalID"], inplace=True)
    set_deltas(context, edits)

    return context


def query_work_order_associated_planting_space(context: dict) -> dict | None:
    layer_id = 2
    key = "PlantingSpaceGlobalID"
    edits = DataFrame(get_deltas(context))

    try:
        plantingSpaces = (
            context["repo"]
            .query(
                [
                    LayerQuery(
                        layer_id,
                        [
                            "Borough",
                            "BuildingNumber",
                            "CityCouncil",
                            "CommunityBoard",
                            "CrossStreet1",
                            "CrossStreet2",
                            "GISPROPNUM",
                            "ParkName",
                            "ParkZone",
                            "PlantingSpaceOnStreet",
                            "StateAssembly",
                            "StreetName",
                            "GlobalID",
                            "OBJECTID",
                        ],
                        f"GlobalID IN ({join(edits[key], True)})",
                    )
                ]
            )[layer_id]
            .rename(
                columns={
                    "GlobalID": "PlantingSpaceGlobalID",
                    "OBJECTID": "PlantingSpaceId",
                    "PlantingSpaceOnStreet": "OnStreetSite",
                }
            )
        )
        __logger.debug(f"Planting Spaces to hydrate: {len(plantingSpaces)}")

    except Exception as e:
        exception_handler(e)

    edits = merge(edits, plantingSpaces, on=key, how="left")
    set_deltas(context, edits)

    return context


def post_work_order_changes(context: dict) -> dict | None:
    edits = get_deltas(context)

    try:
        response = context["service"].post_work_orders(to_json(edits))

    except Exception as e:
        exception_handler(e)

    context["output"]["work_orders_post_changes "] = f"Work Orders result: {response}"
    return context


def get_work_order_edits(context: dict) -> dict | None:
    layer_id = 0
    from_date_time = epoch_to_local_datetime(context["server_gens"].at[0, "WorkOrder"])

    try:
        response = context["service"].get_work_orders(from_date_time)
        __logger.debug(f"WorkOrders Recieved: {len(response)}")

    except Exception as e:
        exception_handler(e)

    if 0 == len(response):
        context["output"]["work_order_get_edits"] = "No Work Order changes."
        return None

    set_deltas(context, {"updates": response}, layer_id)
    return context


def update_work_order_associated_inspection(context: dict) -> dict | None:
    layer_id = 4
    key = "plantingSpaceGlobalId"
    edits = DataFrame(get_deltas(context)["updates"])

    try:
        inspections = (
            context["repo"]
            .query(
                [
                    LayerQuery(
                        layer_id,
                        [
                            "OBJECTID",
                            "HasActiveWorkOrder",
                        ],
                        f"{key} IN ({join(edits[key], True)})",
                    )
                ]
            )[layer_id]
            .rename(columns={"OBJECTID": "InspectionOBJECTID"})
        )
        __logger.debug(f"Inspections To Update: {len(inspections)}")

    except Exception as e:
        exception_handler(e)

    edits = merge(inspections, edits, on=key, how="left")
    edits.loc[
        edits["Status"] == Literal["Closed", "Canceled"], "HasActiveWorkOrder"
    ] = 0
    edits = DataFrame(edits["InspectionOBJECTID", "HasActiveWorkOrder"])
    edits.rename(columns={"InspectionOBJECTID": "OBJECTID"})
    __logger.debug(to_json(edits))

    context["deltas"][layer_id] = {"updates": edits}
    return context


def update_work_order_associated_plantingSpace(context: dict) -> dict:
    layer_id = 2
    key = "plantingSpaceGlobalId"
    edits = DataFrame(get_deltas(context)["updates"])

    try:
        plantingSpaces = context["repo"].query(
            [
                LayerQuery(
                    layer_id,
                    [
                        "GlobalID",
                        "BuildingNumber",
                        "StreetName",
                        "CrossStreet1",
                        "CrossStreet2",
                    ],
                    f"GlobalID IN ({join(edits[key], True)})",
                )
            ]
        )[layer_id]
        __logger.debug(f"Planting Spaces To Update: {len(plantingSpaces)}")

    except Exception as e:
        exception_handler(e)

    update_df(
        edits,
        plantingSpaces,
        key,
        {
            "BuildingNumber": "BuildingNumber",
            "StreetName": "StreetName",
            "CrossStreet1": "CrossStreet1",
            "CrossStreet2": "CrossStreet2",
        },
    )

    context["deltas"][layer_id] = {"updates": plantingSpaces}
    return context


def get_work_order_line_items_edits(context: dict) -> dict | None:
    layer_id = 2
    from_date_time = epoch_to_local_datetime(context["server_gens"].at[0, "WorkOrder"])

    try:
        response = context["service"].get_work_order_line_items(from_date_time)
        __logger.debug(f"Line Items Recieved: {len(response)}")

    except Exception as e:
        exception_handler(e)

    if 0 == len(response):
        context["output"][
            "work_order_line_items_get_edits"
        ] = "No Work Order Line Items Changes."
        return None

    deltas = _seperate_changes(response)
    set_deltas(context, deltas, layer_id)

    return context
