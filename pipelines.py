from logging import getLogger
from ParksGIS.ParksGIS import GISFactory
from eComply.eComply import eComply
from filters import *


try:
    # Configure Logging
    configure_rotating_logger(
        filename="eComply.log",
        directory="eComply_logs",
        level="DEBUG",
        # level="INFO",
    )
    logger = getLogger("[ pipelines ]")

    # Configure Proxy
    proxy = "bcpxy.nycnet:8080"
    configure_proxy(proxy)

    # Set Environemnt
    # arc_gis = "https://formsgisportal.parks.nycnet"
    # arc_gis = 'https://stg-formsgisportal.parks.nycnet'
    arc_gis = "https://dev-formsgisportal.parks.nycnet"

    # Create Dependencies
    factory = GISFactory(
        url=arc_gis + "/portal/home",
        username="forms.python_user",
        password="formsPython24*",
        verify_cert=True if "@" in proxy else False,
    )
    e_comply_repo = factory.create_feature(
        arc_gis + "/server/rest/services/eComply/eComplyContract/FeatureServer"
    )
    data_push_repo = factory.create_feature(
        arc_gis + "/server/rest/services/DataPush/ForMSDataPush/FeatureServer"
    )

    e_comply = eComply(
        url="https://nycparks-stage.ecomply.us/WebAPI",
        username="ff@ecomply.us",
        password="!test123",
    )

    server_gens = query_server_gens(
        {"repo": e_comply_repo},
    )["server_gens"]
    # logger.debug(server_gens)
    # server_gens = mock_server_gens()
    # logger.debug(server_gens)

    logger.info("***Starting Pipelines***")
    logger.info(
        pipeline(
            {
                "message": "**Pushing Contract Domain Values**",
                "service": e_comply,
                "repo": e_comply_repo,
                "layerId": 1,
                "domainNames": [
                    "WOContract",
                    "eComplyContractType",
                    "eComplyContractStatus",
                    "eComplyContractBorough",
                    "eComplyContractFundingSource",
                ],
            },
            query_domains,
            post_domains,
        ).get("output", ""),
    )
    logger.info(
        pipeline(
            {
                "message": "**Pulling Contracts**",
                "service": e_comply,
                "repo": e_comply_repo,
                "server_gens": server_gens.copy(),
            },
            contract_get_edits,
            apply_edits,
        ).get("output", ""),
    )
    logger.info(
        pipeline(
            {
                "message": "**Pushing Work Order Domain Values**",
                "service": e_comply,
                "repo": data_push_repo,
                "layerId": 0,
                "domainNames": [
                    "WOType",
                    "WOStatus",
                    "WOProject",
                    "WOPriority",
                    "BoroughCode",
                    "GenusSpecies",
                ],
            },
            query_domains,
            post_domains,
        ).get("output", ""),
    )
    logger.info(
        pipeline(
            {
                "message": "**Pushing Work Orders**",
                "service": e_comply,
                "repo": data_push_repo,
                "server_gens": server_gens.copy(),
                "server_gens_repo": e_comply_repo,
            },
            # work_order_extract_changes,
            static_workorders,
            wo_query_associated_planting_space_globalid,
            wo_query_associated_planting_space,
            work_order_post_changes,
            apply_server_gens_edits,
        ).get("output", ""),
    )
    logger.info(
        pipeline(
            {
                "message": "**Pulling Work Orders**",
                "service": e_comply,
                "repo": data_push_repo,
                "server_gens": server_gens.copy(),
            },
            work_order_get_edits,
            wo_update_associated_inspection,
            wo_update_associated_platingSpace,
            apply_edits,
        ).get("output", ""),
    )
    logger.info(
        pipeline(
            {
                "message": "**Pulling Work Order Line Items**",
                "service": e_comply,
                "repo": e_comply_repo,
                "server_gens": server_gens.copy(),
            },
            work_order_line_items_get_edits,
            apply_edits,
        ).get("output", ""),
    )
except Exception as e:
    logger.exception(f"Unhandled Exception: {e}")
    raise e
