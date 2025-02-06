from datetime import datetime
from typing import Literal
from pandas import merge


def applyEdits(dict: dict) -> dict:
    if dict.get("applyEdits") is not None and dict["applyEdits"].__len__() > 0:
        dict["server"].apply_edits(dict["applyEdits"].values())
    return dict


def extractChanges(dict: dict) -> dict:
    changes = dict["server"].extractChanges([dict["LayerServerGen"]])

    dict = {
        "applyEdits": {
            3: LayerEdits(
                3,
                updates=[
                    {
                        "attributes": {
                            "OBJECTID": 1,
                            "Contract": changes["layerServerGens"][0]["serverGen"]
                            // 1000,
                        }
                    }
                ],
            )
        },
    }

    objectIds = (
        changes["edits"][0]["objectIds"]["adds"]
        + changes["edits"][0]["objectIds"]["updates"]
    )
    if 0 == objectIds.__len__():
        dict["sendEdits"] = DataFrame()
    else:
        dict["sendEdits"] = server.query([dict["LayerQuery"].getQuery(objectIds)])

    #     print(dict)
    return dict


def contractExtractChanges(
    repo: Server,
    serverGen: int,
    service: eComply,
) -> dict:
    layerId = 1
    changes = repo.extractChanges([LayerServerGen(layerId, serverGen)])

    dict = {
        "service": service,
        "server": repo,
        "serverGen": changes["layerServerGens"][0]["serverGen"] // 1000,
    }

    objectIds = (
        changes["edits"][0]["objectIds"]["adds"]
        + changes["edits"][0]["objectIds"]["updates"]
    )
    if 0 == objectIds.__len__():
        dict["sendEdits"] = DataFrame()
    else:
        dict["sendEdits"] = repo.query(
            [
                LayerQuery(
                    layerId,
                    ["*"],
                    "OBJECTID IN (" + ",".join(str(i) for i in objectIds) + ")",
                    "EcomplyContract=1"
                )
            ]
        )[layerId]

    #     print(dict)
    return dict


def contractSendEdits(dict: dict) -> dict:
    count = dict["sendEdits"].__len__()
    if 0 < count:
        dict["service"].postContracts(
            dict["sendEdits"].to_json(orient="records", date_format="iso")
        )
    print("Contracts Pushed: " + str(count))
    return dict


def workOrderExtractChanges(
    repo: Server,
    serverGen: int,
    service: eComply,
) -> dict:
    layerId = 0
    changes = repo.extractChanges([LayerServerGen(layerId, serverGen)])

    dict = {
        "service": service,
        "server": repo,
        "serverGen": changes["layerServerGens"][0]["serverGen"] // 1000,
    }

    objectIds = (
        changes["edits"][0]["objectIds"]["adds"]
        + changes["edits"][0]["objectIds"]["updates"]
    )
    if 0 == objectIds.__len__():
        dict["sendEdits"] = DataFrame()
    else:
        dict["sendEdits"] = repo.query(
            [
                LayerQuery(
                    layerId,
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
                    "OBJECTID IN (" + ",".join(str(i) for i in objectIds) + ")",
                )
            ]
        )[layerId].rename(
            columns={
                "LocationDetails": "Location",
                "GlobalID": "WorkOrderGlobalId",
                "PROJSTARTDATE": "ProjStartDate",
                "RecommendedSpecies": "RecSpecies",
                "OBJECTID": "ObjectId",
            }
        )

    #     print(dict)
    return dict


def hydrateInspection(dict: dict) -> dict:
    edits = dict["sendEdits"]
    ids = edits[~edits["InspectionGlobalID"].isna()]["InspectionGlobalID"]
    if 0 == ids.__len__():
        return dict

    layerId = 4
    inspections = (
        dict["server"]
        .query(
            [
                LayerQuery(
                    layerId,
                    ["PlantingSpaceGlobalID", "GlobalID"],
                    "GlobalID IN ('" + "','".join(str(id) for id in ids) + "')",
                )
            ]
        )[layerId]
        .rename(columns={"GlobalID": "InspectionGlobalID"})
    )
    del inspections["OBJECTID"]

    edits = dict["sendEdits"] = merge(
        edits, inspections, on="InspectionGlobalID", how="left"
    )
    del edits["InspectionGlobalID"]

    return dict


def hydratePlantingSpace(dict: dict) -> dict:
    edits = dict["sendEdits"]
    ids = edits[~edits["PlantingSpaceGlobalID_x"].isna()]["PlantingSpaceGlobalID_x"]
    if 0 == ids.__len__():
        return dict

    layerId = 2
    plantingSpaces = (
        dict["server"]
        .query(
            [
                LayerQuery(
                    layerId,
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
                    "GlobalID IN ('" + "','".join(str(id) for id in ids) + "')",
                )
            ]
        )[layerId]
        .rename(
            columns={
                "GlobalID": "PlantingSpaceGlobalID",
                "OBJECTID": "PlantingSpaceId",
                "PlantingSpaceOnStreet": "OnStreetSite",
            }
        )
    )

    dict["sendEdits"] = merge(
        edits, plantingSpaces, on="PlantingSpaceGlobalID", how="left"
    )

    return dict


def workOrderSendEdits(dict: dict) -> dict:
    # print(dict["sendEdits"][:2].to_json(orient="records", date_format="iso"))
    count = dict["sendEdits"].__len__()
    if 0 < count:
        dict["service"].postWorkOrders(
            dict["sendEdits"].to_json(orient="records", date_format="iso")
        )
    print("Work Orders Pushed: " + str(dict["sendEdits"].__len__()))
    return dict


def workOrderGetChanges(
    service: eComply,
    repo: Server,
    serverGen: LayerServerGen,
) -> dict:
    fromDateTime = datetime.fromtimestamp(serverGen)
    edits = service.getWorkOrders(fromDateTime)
    return {
        "applyEdits": {0: LayerEdits(0, updates=edits)},
        "server": repo,
    }


def update_associated_inspection_HasActiveWorkOrder(dict: dict) -> dict:
    edits = dict["applyEdits"][0].updates
    ids = edits[~edits["plantingSpaceGlobalId"].isna()]["plantingSpaceGlobalId"]
    if 0 == ids.__len__():
        return dict

    layerId = 4
    inspections = (
        dict["server"]
        .query(
            [
                LayerQuery(
                    layerId,
                    [
                        "PlantingSpaceGlobalID",
                        "HasActiveWorkOrder",
                    ],
                    "PlantingSpacGlobalID IN ('"
                    + "','".join(str(id) for id in ids)
                    + "')",
                )
            ]
        )[layerId]
        .rename(
            columns={
                "PlantingSpaceGlobalID": "plantingSpaceGlobalId",
            }
        )
    )

    edits = merge(inspections, edits, on="plantingSpaceGlobalId", how="left")
    edits.loc[
        edits["Status"] == Literal["Closed", "Canceled"], "HasActiveWorkOrder"
    ] = 0

    dict["applyEdits"][layerId](
        LayerEdits(
            layerId,
            updates=edits[
                "OBJECTID",
                "HasActiveWorkOrder",
            ].to_json(
                orient="records",
            ),
        ),
    )
    return dict


def update_associated_platingSpace_Address(dict: dict) -> dict:
    edits = dict["applyEdits"][0].updates
    ids = edits[~edits["plantingSpaceGlobalId"].isna()]["plantingSpaceGlobalId"]
    if 0 == ids.__len__():
        return dict

    layerId = 2
    plantingSpaces = (
        dict["server"]
        .query(
            [
                LayerQuery(
                    layerId,
                    [
                        "GlobalID",
                        "BuildingNumber",
                        "StreetName",
                        "CrossStreet1",
                        "CrossStreet2",
                    ],
                    "GlobalID IN ('" + ",".join(str(id) for id in ids) + "')",
                )
            ]
        )[layerId]
        .rename(
            columns={
                "GlobalID": "plantingSpaceGlobalId",
            }
        )
    )

    Transformer.update(
        plantingSpaces,
        edits,
        "plantingSpaceGlobalId",
        {
            "BuildingNumber": {"Source": "BuildingNumber"},
            "StreetName": {"Source": "StreetName"},
            "CrossStreet1": {"Source": "CrossStreet1"},
            "CrossStreet2": {"Source": "CrossStreet2"},
        },
    )

    dict["applyEdits"][layerId](
        LayerEdits(
            layerId,
            updates=plantingSpaces.to_json(orient="records"),
        ),
    )
    return dict


def contractGetChanges(
    service: eComply,
    repo: Server,
    serverGen: int,
) -> dict:
    fromDateTime = datetime.fromtimestamp(serverGen)
    edits = service.getContracts(fromDateTime)
    return {
        "applyEdits": {
            1: LayerEdits(1, updates=edits),
        },
        "server": repo,
    }


def workOrderLineItemGetChanges(
    service: eComply,
    repo: Server,
    serverGen: int,
) -> dict:
    fromDateTime = datetime.fromtimestamp(serverGen)
    edits = service.getWorkOrderLineItems(fromDateTime)
    return {
        "applyEdits": {
            2: LayerEdits(2, updates=edits),
        },
        "server": repo,
    }
