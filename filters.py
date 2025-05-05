from datetime import datetime
from inspect import currentframe
from json import dumps
from logging import Logger, config, getLogger
from math import isnan
from numpy import ndarray
from pandas import DataFrame, Series, merge
from typing import Any, Callable, Literal
from ParksGIS import (
    LayerDomainNames,
    LayerEdits,
    LayerQuery,
    LayerServerGen,
    Server,
)

__logger: Logger = getLogger("[ filters ]")


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


def epoch_to_local_datetime(epoch: int) -> datetime:
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
    return data[data[key].notna()]


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
    if not funcs:
        raise ValueError("At least one function must be provided.")

    if "log" in context:
        __logger.info(context["log"])

    context["output"] = {}

    result = context
    for _, func in enumerate(funcs):
        if result is None:
            __logger.debug(f"*Pipeline ended before {func.__name__}*")
            break

        if not isinstance(func, Callable):
            raise TypeError(
                f"Expected a callable for '{func if isinstance(func, str) else func.__name__ or func}'"
            )

        __logger.debug(f"Executing: {func.__name__}")
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
        if isnan(item["objectId"]):
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
        __logger.debug(f"Domain Values count: {len(context)}")
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

    context["output"]["post_domains"] = f"Domain Values result: {response}"
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


# Work Order
def work_order_extract_changes(context: dict) -> dict | None:
    layer_id = 0

    try:  # TODO refactor for clarity: only workorders with contract
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
        __logger.debug(
            f"Work Orders Extracted: {0 if result['changes'].empty else len(result['changes'])}"
        )

    except Exception as e:
        exception_handler(e)

    if result["changes"] is None:
        context["output"]["work_order_extract_changes"] = "No Work Order changes."
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
        __logger.debug(f"Planting Space Ids found: {len(inspections)}")

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
        __logger.debug(f"Planting Spaces to hydrate: {len(plantingSpaces)}")

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

    context["output"]["work_orders_post_changes "] = f"Work Orders result: {response}"
    return context


# Receiving
def work_order_get_edits(context: dict) -> dict | None:
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


def work_order_line_items_get_edits(context: dict) -> dict | None:
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


#####################################################################################


# Test data
def mock_server_gens():
    import time

    seconds = 60 * 60 * 24 * 30  # 5 minutes
    epoch: int = round(time.time() - seconds) * 1000

    server_gens = DataFrame(
        {
            "OBJECTID": 1,
            "Contract": [epoch],
            "WorkOrder": [epoch],
        }
    )

    return server_gens


def static_workorders(context: dict) -> dict | None:
    layer_id = 0

    data = context["repo"].query(
        [
            LayerQuery(
                layer_id,
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
                "OBJECTID IN (630610,630617,631004,631047,631414,631419,631824,631827,632609,634646,635090,636648,651829,652124,655689,789854,1663933,2583357,2880592,2880594,2977446,2980622,3412678,3421471,3427069,4217103,5756513,7260528,7294892,7349280,7353018,7353019,7353020,7353021,7353022,7415031,7415037,7428788,7428791,7449912,7453515,7634271,7636269,7750947,7752948,7912282,7912283,7912284,8075277,8236118,8236186,8541626,8581033,8596562,8601717,8661646,8702362,8705564,8710364,8710365,8711962,8722780,8759275,8759280,8759281,8759283,8759286,8759289,8771986,8772386,8776787,8777215,8795237,8935321,8936945,8936954,8936959,8937321,8962164,9030228,9056230,9131222,9138859,9138875,9138878,9138881,9138885,9140189,9140190,9145627,9145632,9145636,9149799,9149803,9150127,9157321,9157331,9157348,9171637,9172503,9188429,9245901,9260710,9946820,13266576,13334033,13337955,18010892,18463957,18464354,19052401,21285405,21285409,22298819,22298822,22298823,22298824,22298825,22298826,22298827,22298828,22298830,22298831,22298834,22298835,22298836,22468789,22955891,22955893,22955895,22955900,22955905,22955908,22955912,22955916,22975699,22975700,22975703,22975706,22975710,22977680,22977681,22977686,22981803,23025315,23040785,23040787,23040788,23040793,23040798,23040801,23040807,23040809,23041303,23041307,23047546,23047547,23056060,23273348,23273350,23273352,23273353,23273378,23276280,23276327,23276328,23277502,23304113,23304114,23304115,23304121,23304124,23304125,23304126,23304127,23304146,23304154,23304157,23304971,23304972,23305377,23316781,23316782,23316785,23318075,23318078,23318090,23318100,23318146,23318150,23318161,23318164,23318166,23318167,23318180,23318195,23318199,23318200,23318201,23318217,23318218,23318221,23332847,23332860,23335329,23335337,23335340,23335342,23335344,23335348,23335351,23335364,23335373,23336156,23441223,23462352,23490236,23490248,23490269,23608249,23608251,23608253,23608254,23608257,23608258,23626744,23626751,23626755,23626756,23638331,23638333,23660099,24195498,24318771,24318772,24319646,24320933,24320934,24329947,24329948,24329949,24329954,24330742,24350807,24350811,24350814,24350826,24350827,24357131,24357132,24357140,24357141,24357142,24357143,24357144,24357146,24358371,24367164,24367166,24367167,24368477,24368479,24368660,24395529,24395530,24414079,24414080,24414081,24414082,24414083,24415679,24415680,24415681,24415682,24415683,24415684,24419647,24419648,24438505,24451747,24508940,24508942,24508957,24508958,24508959,24508961,24515857,24533598,24533599,24533605,24533606,24533607,24533608,24533635,24551500,24573458,24573465,24573466,24573468,24578812,24585353,24585374,24585375,24585378,24585379,24585380,24585395,24585396,24585397,24585398,24585399,24585400,24585405,24585411,24585412,24585413,24585420,24585421,24587530,24620899,24620901,24652803,24654979,24654990,24657179,24657181,24722996,24735670,24735672,24735677,24738199,24738212,24738214,24738219,24738227,24738256,24738259,24738260,24738261,24738262,24738264,24738266,24738267,24738269,24738272,24738274,24738277,24738278,24738313,24738315,24738316,24738318,24738319,24738320,24738323,24738325,24738329,24738330,24738331,24738333,24738335,24738337,24738338,24738339,24738340,24738341,24738342,24738344,24738346,24738347,24738349,24738350,24738353,24833664,24833665,24833667,24833668,24833670,24833671,24833673,24833674,24833679,24833682,24833686,24833689,24833692,24833694,24833697,24833722,24835757,24835759,24869434,24869435,24869436,24884513,24884520,24887871,24887910,24887913,24887914,24887915,24887916,24887918,24887920,24887921,24888310,24897245,24897988,24897989,24897996,24898981,24905495,24915476,24915477,24915478,24915480,24915481,24915482,24915483,24915485,24915486,24939030,24939031,24939035,24939038,24939039,24939040,24939041,24939042,24939043,24939044,24939045,24939046,24939047,24939048,24939049,24939051,24939424,24943071,24943089,24943090,24943091,24943092,24943093,24943094,24943095,24943096,24943097,24952782,24952783,24952784,24952785,24952786,24952791,24952793,24952794,24952796,24952798,24952799,24952800,24952801,24952802,24952803,24952804,24952806,24952809,25191598,25193026,25193027,25193028,25198959,25199004,25199006,25272244,25272254,25272261,25272262,25272263,25272264,25272265,25272266,25272267,25272268,25272269,25272270,25272271,25272456,25272466,25272468,25300728,25300731,25341341,25341342,25398515,25454159,25454166,26099728,26099729,26099730,26099731)",
            )
        ]
    )[layer_id]

    set_deltas(context, data, layer_id)
    __logger.debug(f"Static Work Orders Extracted: {len(data)}")

    return context
