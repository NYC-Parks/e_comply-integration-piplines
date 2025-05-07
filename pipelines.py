from logging import getLogger
from ParksGIS import GISFactory
from filters import *
import eComply

__logger = getLogger("[ pipelines ]")


try:
    configure_rotating_logger(
        filename="eComply.log",
        directory="eComply_logs",
        level="DEBUG",
        # level="INFO",
    )

    credential = ""
    proxy_url = "bcpxy.nycnet:8080"
    set_proxy_variables(credential + proxy_url)

    # Set ArcGIS Environemnt
    # arc_gis = "https://formsgisportal.parks.nycnet"
    # arc_gis = 'https://stg-formsgisportal.parks.nycnet'
    arc_gis = "https://dev-formsgisportal.parks.nycnet"

    factory = GISFactory(
        url=f"{arc_gis}/portal/home",
        username="forms.python_user",
        password="formsPython24*",
        verify_cert=False if credential == "" else True,
    )
    e_comply_repo = factory.create("eComply/eComplyContract/FeatureServer")
    data_push_repo = factory.create("DataPush/ForMSDataPush/FeatureServer")

    # Set eComply Environemnt
    # e_comply = "https://nycparks-test.ecomply.us"
    e_comply = "https://nycparks-stage.ecomply.us"

    e_comply_service = eComply.API(
        url=f"{e_comply}/WebAPI",
        username="ff@ecomply.us",
        password="!test123",
    )

    server_gens = query_server_gens(
        {"repo": e_comply_repo},
    )["server_gens"]
    # server_gens = mock_server_gens()
    __logger.debug(f"ServerGens: {server_gens}")

    __logger.info("***Starting Pipelines***")
    __logger.info(
        pipeline(
            {
                "message": "**Pushing Contract Domain Values**",
                "service": e_comply_service,
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
    __logger.info(
        pipeline(
            {
                "message": "**Pulling Contracts**",
                "service": e_comply_service,
                "repo": e_comply_repo,
                "server_gens": server_gens.copy(),
            },
            contract_get_edits,
            apply_edits,
        ).get("output", ""),
    )
    __logger.info(
        pipeline(
            {
                "message": "**Pushing Work Order Domain Values**",
                "service": e_comply_service,
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
    __logger.info(
        pipeline(
            {
                "message": "**Pushing Work Orders**",
                "service": e_comply_service,
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
    __logger.info(
        pipeline(
            {
                "message": "**Pulling Work Orders**",
                "service": e_comply_service,
                "repo": data_push_repo,
                "server_gens": server_gens.copy(),
            },
            work_order_get_edits,
            wo_update_associated_inspection,
            wo_update_associated_plantingSpace,
            apply_edits,
        ).get("output", ""),
    )
    __logger.info(
        pipeline(
            {
                "message": "**Pulling Work Order Line Items**",
                "service": e_comply_service,
                "repo": e_comply_repo,
                "server_gens": server_gens.copy(),
            },
            work_order_line_items_get_edits,
            apply_edits,
        ).get("output", ""),
    )
except Exception as e:
    __logger.exception(f"Unhandled Exception: {e}")
    raise e
