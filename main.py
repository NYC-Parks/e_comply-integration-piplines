from logging import getLogger
from filters import *
from eComply import API
from ParksGIS import GISFactory
from filters_test_data import *

__logger = getLogger("[ main ]")


try:
    configure_rotating_logger(
        filename="eComply.log",
        directory="logs",
        level="DEBUG",
        # level="INFO",
    )

    credential = ""
    proxy_url = "http://bcpxy.nycnet:8080"
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

    e_comply_service = API(
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
                "log": "**Pushing Contract Domain Values**",
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
                "log": "**Pulling Contracts**",
                "service": e_comply_service,
                "repo": e_comply_repo,
                "server_gens": server_gens.copy(),
            },
            get_contract_edits,
            apply_edits,
        ).get("output", ""),
    )
    __logger.info(
        pipeline(
            {
                "log": "**Pushing Work Order Domain Values**",
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
                "log": "**Pushing Work Orders**",
                "service": e_comply_service,
                "repo": data_push_repo,
                "server_gens": server_gens.copy(),
                "server_gens_repo": e_comply_repo,
            },
            query_contract_ids,
            contract_associated_work_order_extract_changes,
            # static_workorders,
            query_work_order_associated_planting_space_globalid,
            query_work_order_associated_planting_space,
            post_work_order_changes,
            apply_server_gens_edits,
        ).get("output", ""),
    )
    __logger.info(
        pipeline(
            {
                "log": "**Pulling Work Orders**",
                "service": e_comply_service,
                "repo": data_push_repo,
                "server_gens": server_gens.copy(),
            },
            get_work_order_edits,
            update_work_order_associated_inspection,
            update_work_order_associated_plantingSpace,
            apply_edits,
        ).get("output", ""),
    )
    __logger.info(
        pipeline(
            {
                "log": "**Pulling Work Order Line Items**",
                "service": e_comply_service,
                "repo": e_comply_repo,
                "server_gens": server_gens.copy(),
            },
            get_work_order_line_items_edits,
            apply_edits,
        ).get("output", ""),
    )
except Exception as e:
    __logger.exception(f"Unhandled Exception: {e}")
    raise e
