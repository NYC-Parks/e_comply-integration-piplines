from datetime import datetime
from inspect import currentframe
from json import dumps
from logging import config, getLogger
from numpy import ndarray
from pandas import DataFrame, Series, merge
from typing import Any, Callable, Literal

from ParksGIS.ParksGIS import LayerDomainNames, LayerEdits, LayerQuery, LayerServerGen, Server

filter_logger = getLogger("[ filters ]")


# Generic
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


def epoch_to_datetime(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch / 1000)


def to_json(obj: Any) -> str:
    if isinstance(obj, DataFrame):
        return obj.to_json(orient="records", date_format="iso")
    else:
        return dumps(obj)


def join(
    list: Series | list,
    withQuotations: bool = False,
) -> str:
    if withQuotations:
        return "'" + "','".join(str(i) for i in list) + "'"
    else:
        return ",".join(str(i) for i in list)


def filter_Nones(data: DataFrame | Series, key: str) -> DataFrame | Series:
    return data[~data[key].isna()]


def update_df(
    destination: DataFrame,
    source: DataFrame,
    key: str,
    map: dict[str, str],
) -> DataFrame:
    unhasablekey = False
    keyType = source.at[0, key]

    # create temp key for unhashable arrays
    if isinstance(keyType, list) or isinstance(keyType, ndarray):
        unhasablekey = True

        destinationKey = key + "dest"
        destination[destinationKey] = destination[key].apply(
            lambda x: "".join(str(i) for i in x)
        )

        sourceKey = key + "src"
        source[sourceKey] = source[key].apply(lambda x: "".join(str(i) for i in x))

    merged = destination.merge(
        source,
        left_on=destinationKey,
        right_on=sourceKey,
        how="left",
        suffixes=(None, "_right"),
        indicator=True,
    )

    # map source columns to destination columns
    for dest_key, src_key in map.items():
        true_key = src_key + ("_right" if dest_key == src_key else "")
        merged[dest_key] = merged[true_key]

    # remove all added columns
    removeCols = source.columns.to_list()
    removeCols.append("_merge")
    if unhasablekey:
        removeCols.append(destinationKey)
        removeCols.append(sourceKey)

    result = merged.drop(columns=removeCols)

    renameCols = {}
    for item in result.columns:
        if "_right" in item:
            renameCols[item] = item.replace("_right", "")

    return result.rename(columns=renameCols)


def pipeline(
    context: dict[str, Any],
    *funcs: Callable[
        [dict[str, Any]],
        dict[str, Any] | None,
    ],
) -> dict[str, Any]:
    if not funcs:
        raise ValueError("At least one function must be provided.")

    if "message" in context:
        filter_logger.info(context["message"])

    result = context

    for _, func in enumerate(funcs):
        if result is None:
            filter_logger.debug(f"*Pipeline ended before {func.__name__}*")
            break

        if not isinstance(func, Callable):
            raise TypeError(f"Expected a callable for {func.__name__}")

        filter_logger.debug(f"Executing: {func.__name__}")
        result = func(result)

    return context


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
                    "backupCount": 7,  # Keep last 7 logs
                    "encoding": "utf-8",
                },
                "console": {
                    "class": "logging.StreamHandler",
                    "level": level,
                    "formatter": "detailed",
                },
            },
            "root": {
                "level": "DEBUG",
                "handlers": ["timed_rotating_file", "console"],
            },
        }
    )

    # Enable http debug output
    import http.client

    http.client.HTTPConnection.debuglevel = 1 if level == "DEBUG" else 0


def configure_proxy(proxy: str) -> None:
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
            filter_logger.debug(f"LayerEdits: {id}, {edits}")
            layer_edits.append(
                LayerEdits(
                    id,
                    adds=edits.get("adds", None),
                    updates=edits.get("updates", None),
                )
            )

        try:
            result = context["repo"].apply_edits(layer_edits)
            filter_logger.debug(f"Edits Applied: {to_json(result)}")

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


def seperate_changes(changes: list) -> dict[str, DataFrame]:
    columns = list(changes[0].keys())
    adds = DataFrame(columns=columns)
    updates = DataFrame(columns=columns)
    deltas: dict[str, DataFrame] = {}

    for item in changes:
        if item["objectId"] is None:
            adds.loc[len(adds)] = item
        else:
            updates.loc[len(updates)] = item

    filter_logger.debug(f"Adds: {len(adds)}")
    if 0 < len(adds):
        deltas["adds"] = adds.drop(columns=["objectId"])
    filter_logger.debug(f"Updates: {len(updates)}")
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
        filter_logger.debug("Server Gens saved")

    except Exception as e:
        exception_handler(e)

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
        filter_logger.debug(f"Domain Values count: {len(context)}")
    except Exception as e:
        exception_handler(e)

    return context


def post_domains(context: dict) -> dict | None:
    values = [
        {
            "domainName": domain["name"],
            "code": str(value["code"]),
            "value": value["name"],
        }
        for domain in context["domainValues"]
        for value in domain["codedValues"]
    ]

    try:
        response = context["service"].post_domain_values(values)

    except Exception as e:
        exception_handler(e)

    context["output"] = f"Domain Values result: {response}"
    return context


#######################################################################################


# Contract
# def contract_extract_changes(context: dict) -> dict | None:
#     layer_id = 1

#     try:
#         result = extract_changes(
#             context["repo"],
#             layer_id,
#             context["server_gens"].at[0, "Contract"],
#             [
#                 "OBJECTID",
#                 "ContractName",
#                 "ContractType",
#                 "ContractStatus",
#                 "Borough",
#                 "FundingSource",
#                 "ProjectManager",
#                 "Contractor",
#                 "AwardAmount",
#                 "AwardDate",
#                 "OrderToWorkDate",
#                 "SpecifiedCompletionDate",
#                 "AnticipatedCompletionDate",
#                 "CreatedDate",
#                 "CreatedByName",
#                 "CreatedBYERN",
#                 "UpdatedDate",
#                 "UpdatedByName",
#                 "UpdatedByERN",
#                 "ClosedDate",
#                 "ClosedByName",
#                 "ClosedByERN",
#                 "CancelDate",
#                 "CancelByName",
#                 "CancelByERN",
#                 "CancelReason",
#                 "ClosedBySystem",
#             ],
#             "EcomplyContract = 1",
#         )
#         filter_logger.debug(
#             f"Contracts Extracted: {len(result['changes']) if result['changes'] else 0}"
#         )

#     except Exception as e:
#         exception_handler(e)

#     if result["changes"] is None:
#         context["output"] = "No Contract changes."
#         return None

#     set_edits(context, result["changes"], layer_id)
#     context["server_gens"].at[0, "Contract"] = result["server_gen"]
#     return context


# def contract_post_changes(context: dict) -> dict:
#     edits = get_edits(context)

#     try:
#         response = context["service"].post_contracts(edits)

#     except Exception as e:
#         exception_handler(e)

#     context["output"] = f"Contracts result: {response}"
#     return context


def contract_get_edits(context: dict) -> dict | None:
    layer_id = 1
    from_date_time = epoch_to_datetime(context["server_gens"].at[0, "Contract"])

    try:
        response = context["service"].get_contracts(from_date_time)
        filter_logger.debug(f"Contracts Recieved: {len(response)}")

    except Exception as e:
        exception_handler(e)

    if 0 == len(response):
        context["output"] = "No Contract Changes."
        return None

    deltas = seperate_changes(response)
    set_deltas(context, deltas, layer_id)

    return context


# Work Order
def work_order_extract_changes(context: dict) -> dict | None:
    layer_id = 0

    try:
        contract_ids = context["server_gens_repo"].query(
            [
                LayerQuery(
                    1,
                    ["ContractName"],
                )
            ]
        )[1]["ContractName"]

        result = extract_changes(
            context["repo"],
            layer_id,
            context["server_gens"].at[0, "WorkOrder"],
            [
                "InspectionGlobalID",
                "Type",
                "Status",
                "LocationDetails",
                "ActualFinishDate",
                "Comments",
                "Contract",
                "CancelReason",
                "GlobalID",
                "ClosedDate",
                "ClosedByERN",
                "ClosedByName",
                "CancelDate",
                "CancelByERN",
                "CancelByName",
                "CreatedDate",
                "CreatedBYERN",
                "CreatedByName",
                "UpdatedDate",
                "UpdatedByERN",
                "UpdatedByName",
                "WOEntity",
                "PROJSTARTDATE",
                "Project",
                "RecommendedSpecies",
                "ClosedBySystem",
                "OBJECTID",
            ],
            f"Contract in ({join(contract_ids)})",
        )
        filter_logger.debug(
            f"Work Orders Extracted: {0 if result['changes'].empty else len(result['changes'])}"
        )

    except Exception as e:
        exception_handler(e)

    if result["changes"] is None:
        context["output"] = "No Work Order changes."
        return None

    set_deltas(context, result["changes"], layer_id)
    context["server_gens"].at[0, "WorkOrder"] = result["server_gen"]
    return context


def wo_query_associated_planting_space_globalid(context: dict) -> dict | None:
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
                        ["PlantingSpaceGlobalID", "GlobalID"],
                        f"GlobalID IN ({join(filter_Nones(edits, key)[key], True)})",
                    )
                ]
            )[layer_id]
            .rename(columns={"GlobalID": key})
        )
        filter_logger.debug(f"Planting Space Ids found: {len(inspections)}")

    except Exception as e:
        exception_handler(e)

    edits = merge(edits, inspections, on=key, how="left")
    set_deltas(context, edits)

    return context


def wo_query_associated_planting_space(context: dict) -> dict | None:
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
                            "ParkName",
                            "ParkZone",
                            "Borough",
                            "CommunityBoard",
                            "BuildingNumber",
                            "StreetName",
                            "CityCouncil",
                            "StateAssembly",
                            "GISPROPNUM",
                            "CrossStreet1",
                            "CrossStreet2",
                            "PlantingSpaceOnStreet",
                            "ObjectID",
                            "GlobalID",
                        ],
                        f"GlobalID IN ({join(filter_Nones(edits, key)[key], True)})",
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
        filter_logger.debug(f"Planting Spaces to hydrate: {len(plantingSpaces)}")

    except Exception as e:
        exception_handler(e)

    edits = merge(edits, plantingSpaces, on=key, how="left")
    set_deltas(context, edits)

    return context


def work_order_post_changes(context: dict) -> dict | None:
    edits = get_deltas(context)

    try:
        response = context["service"].post_work_orders(edits)

    except Exception as e:
        exception_handler(e)

    context["output"] = f"Work Orders result: {response}"
    return context


# Receiving
def work_order_get_edits(context: dict) -> dict | None:
    layer_id = 0
    from_date_time = epoch_to_datetime(context["server_gens"].at[0, "WorkOrder"])

    try:
        response = context["service"].get_work_orders(from_date_time)
        filter_logger.debug(f"WorkOrders Recieved: {len(response)}")

    except Exception as e:
        exception_handler(e)

    if 0 == len(response):
        context["output"] = "No Work Order changes."
        return None

    set_deltas(context, {"updates": response}, layer_id)
    return context


def wo_update_associated_inspection(context: dict) -> dict | None:
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
        filter_logger.debug(f"Inspections To Update: {len(inspections)}")

    except Exception as e:
        exception_handler(e)

    edits = merge(inspections, edits, on=key, how="left")
    edits.loc[
        edits["Status"] == Literal["Closed", "Canceled"], "HasActiveWorkOrder"
    ] = 0
    edits = DataFrame(edits["InspectionOBJECTID", "HasActiveWorkOrder"])
    edits.rename(columns={"InspectionOBJECTID": "OBJECTID"})
    filter_logger.debug(to_json(edits))

    context["deltas"][layer_id] = {"updates": edits}
    return context


def wo_update_associated_platingSpace(context: dict) -> dict:
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
        filter_logger.debug(f"Planting Spaces To Update: {len(plantingSpaces)}")

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


def work_order_line_items_get_edits(context: dict) -> dict | None:
    layer_id = 2
    from_date_time = epoch_to_datetime(context["server_gens"].at[0, "WorkOrder"])

    try:
        response = context["service"].get_work_order_line_items(from_date_time)
        filter_logger.debug(f"Line Items Recieved: {len(response)}")

    except Exception as e:
        exception_handler(e)

    if 0 == len(response):
        context["output"] = "No Work Order Line Items Changes."
        return None

    deltas = seperate_changes(response)
    set_deltas(context, deltas, layer_id)

    return context