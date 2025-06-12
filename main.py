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

    __logger.info("**Pushing Contract Domain Values**")
    pipeline(
        {
            "service": e_comply_service,
            "repo": e_comply_repo,
            "layer_id": 1,
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
    )

    __logger.info("**Pulling Contracts**")
    pipeline(
        {
            "service": e_comply_service,
            "repo": e_comply_repo,
            "server_gens": server_gens.copy(),
        },
        get_contract_edits,
        seperate_contract_edits,
        apply_edits,
    )

    __logger.info("**Pushing Work Order Domain Values**")
    pipeline(
        {
            "service": e_comply_service,
            "repo": data_push_repo,
            "layer_id": 0,
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
    )

    __logger.info("**Pushing Work Orders**")
    pipeline(
        {
            "service": e_comply_service,
            "repo": data_push_repo,
            "server_gens": server_gens.copy(),
            "server_gens_repo": e_comply_repo,
        },
        query_contract_ids,
        query_contract_associated_work_order,
        # static_workorders,
        query_work_order_associated_planting_space_globalid,
        query_work_order_associated_planting_space,
        post_work_order_changes,
        apply_server_gens_edits,
    )

    __logger.info("**Pulling Work Orders**")
    pipeline(
        {
            "service": e_comply_service,
            "repo": data_push_repo,
            "layer_id": 0,
            "server_gens": server_gens.copy(),
            "domainNames": ["WOStatus"],
        },
        get_work_order_edits,
        query_domains,
        query_associated_inspections,
        update_inspections,
        query_associated_planting_spaces,
        update_planting_spaces,
        apply_edits,
    )

    __logger.info("**Pulling Line Items**")
    pipeline(
        {
            "service": e_comply_service,
            "repo": e_comply_repo,
            "layer_id": 2,
            "server_gens": server_gens.copy(),
        },
        get_line_item_edits,
        separate_line_item_edits,
        apply_edits,
    )
except Exception as e:
    __logger.exception(f"Unhandled Exception: {e}")
    raise e
