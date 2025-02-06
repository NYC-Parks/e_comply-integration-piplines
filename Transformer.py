from datetime import datetime
from functools import singledispatch
from typing import Any, Callable, List, Literal, Optional, Union
from numpy import ndarray
from pandas import DataFrame, Series
from arcgis.features import GeoAccessor


class MapExpr(dict):
    def __init__(
        self,
        dict: Union[
            dict[Literal["Func"], Callable[[Any], Any]],
            dict[Literal["Source"], str],
            dict[Literal["Value"], Union[str, int, None]],
        ],
    ):
        if 1 != dict.keys().__len__():
            raise Exception("Dictionary must have one key!")
        self = dict


Map = dict[
    str,
    tuple[
        Literal["Func", "Source", "Value"],
        Union[Callable[[Any], Any], float, str, None],
    ],
]


class Transformer:
    @staticmethod
    @singledispatch
    def validate(_, type, __) -> Any:
        if type not in ["Func", "Source", "Value"]:
            raise Exception(f"Unsupported expression type: {type}")

    @staticmethod
    @validate.register(Callable)
    def _(expr: Callable, _, key: str):
        if not isinstance(expr, Callable):
            raise Exception(f"Function not callable: {key}")

    @staticmethod
    @validate.register(str)
    def _(expr: str, _, __):
        if not isinstance(expr, str):
            raise Exception(f"Invalid source column: {expr}")

    @staticmethod
    def getValues(
        source: DataFrame,
        map: Union[Map, tuple],
    ) -> DataFrame:
        result = DataFrame()

        if isinstance(map, tuple):
            map = dict(map)

        for key, (type, expr) in map.items():
            Transformer.validate(None, type, None)

            if type == "Func":
                Transformer.validate(expr, type, key)

                result[key] = source[key].apply(expr)

            elif type == "Source":
                Transformer.validate(expr, type, key)

                if "+" in expr:
                    columns = [col.strip() for col in expr.split("+")]
                    result[key] = source[columns].agg(lambda x: "".join(x.map(str)), 1)
                else:
                    result[key] = source[expr].values

            else:
                if isinstance(expr, str) and expr == "utcNow":
                    result[key] = datetime.utcnow()
                else:
                    result[key] = expr

        return result

    @staticmethod
    def update(
        destination: DataFrame,
        source: DataFrame,
        key: str,
        map: Map,
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

            sourceKey = key + "sour"
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
        for item in map.items():
            for col, values in Transformer.getValues(
                merged,
                item,
            ).items():
                merged[col] = values

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

    @staticmethod
    def createFromDF(
        source: DataFrame,
        map: Map,
        x: Optional[str] = None,
        y: Optional[str] = None,
    ) -> DataFrame:
        result = DataFrame()

        for item in map.items():
            value = Transformer.getValues(
                source,
                item,
            )

            if (
                isinstance(value, List)
                or isinstance(value, ndarray)
                or isinstance(value, Series)
            ):
                result[item] = value
            else:
                result[item] = [value] * source.shape[0]

        if x is not None and y is not None:
            result = GeoAccessor.from_xy(result, x, y, sr=2263)

        return result
