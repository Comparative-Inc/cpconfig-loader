from dataclasses import dataclass, field
from enum import Enum, auto
from functools import cached_property
from typing import Any, Dict, List, Optional, Set, Union

import sqlglot
from sqlglot import expressions


class MaterializeIncrementalType(Enum):
    BIGQUERY = auto()
    REDSHIFT = auto()


class ColumnType(Enum):
    CONTINUOUS = auto()
    CATEGORICAL = auto()
    BOOLEAN = auto()


class ContinuousValueFormatter(Enum):
    DAYS = auto()
    MINUTES = auto()


@dataclass(frozen=True)
class Profile:
    materialize_incremental_type: MaterializeIncrementalType


@dataclass(frozen=True)
class SyncFrom:
    tap_name: str
    tap_url: Optional[str]
    credential_key: str
    config: Optional[Dict[str, Any]]


@dataclass(frozen=True)
class Source:
    name: str
    table: Optional[str]
    create_view: Optional[str]
    cp_user_id: Optional[str]
    cp_date: Optional[str]
    where: Optional[str]
    where_incremental: Optional[str]
    join_using: Optional[List[str]]
    group_by: Optional[List[str]]
    sync_from: Optional[SyncFrom]

    def __post_init__(self) -> None:
        mode = list(filter(None, [self.table, self.create_view, self.sync_from]))
        if len(mode) != 1:
            raise ValueError(
                "A source must have exactly either 'table' or 'create_view' or 'sync_from' field"
            )
        if not self.join_using_columns:
            raise ValueError("A source must have either cp_user_id, cp_date, or join_using")

    def __str__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.name)

    @cached_property
    def group_by_columns(self) -> List[str]:
        if self.group_by is not None:
            return self.group_by
        return ["cp_user_id", "cp_date"] if self.cp_user_id and self.cp_date else []

    @cached_property
    def join_using_columns(self) -> List[str]:
        return (
            self.join_using
            or self.group_by
            or list(
                filter(
                    None,
                    [
                        "cp_user_id" if self.cp_user_id else None,
                        "cp_date" if self.cp_date else None,
                    ],
                )
            )
        )

    @property
    def dbt_ref(self) -> str:
        if self.table:
            return self.table
        elif self.sync_from:
            return "{{ target.schema }}." + self.name

        return "{{ ref('" + self.name + "') }}"


@dataclass(frozen=True)
class MetricDisplayConfig:
    unit: str
    is_unit_prefix: bool = False
    participant_singular: str = "participant"
    participant_plural: str = "participants"
    is_pct: bool = False

    def __str__(self) -> str:
        return self.unit


@dataclass(frozen=True)
class Metric:
    name: str
    per_row_select: str
    aggregate_select: str
    per_row_pandas: str
    aggregate_pandas: str
    per_row_column_type: ColumnType
    breakdown: Optional[str]
    display: MetricDisplayConfig
    root_predicate: Optional[str] = None
    is_target: bool = True
    forced_dimensions: List[str] = field(default_factory=list)

    @cached_property
    def required_column_names(self) -> Set[str]:
        return set(
            map(
                lambda c: c.this.this,
                sqlglot.parse_one(self.per_row_select).find_all(expressions.Column),
            )
        ) | set(
            map(
                lambda c: c.this.this,
                sqlglot.parse_one(self.aggregate_select).find_all(expressions.Column),
            )
        )


@dataclass(frozen=True)
class Segmentation:
    type: ColumnType
    humanize: Optional[Union[str, List[str]]] = None
    value_formatter: Optional[ContinuousValueFormatter] = None

    def __post_init__(self) -> None:
        if self.humanize is not None:
            if self.value_formatter and self.type != ColumnType.CONTINUOUS:
                raise ValueError("Only CONTINUOUS columns can have value_formatter")
            if isinstance(self.humanize, str) and self.type == ColumnType.BOOLEAN:
                raise ValueError(
                    f"CONTINUOUS and CATEGORICAL columns take a string humanize, BOOLEAN columns take a list of strings (for true and false cases). Please check {self.humanize}"
                )


@dataclass(frozen=True)
class Dimension:
    name: str
    select: str
    source: Optional[str]
    default: Optional[str]
    where: Optional[str]
    segmentation: Optional[Segmentation]
    parent: Optional[str]

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return self.name

    @property
    def is_derived(self) -> bool:
        return self.source is None

    @cached_property
    def has_aggregate_func(self) -> bool:
        return sqlglot.parse_one(self.select).find(sqlglot.expressions.AggFunc) is not None

    @cached_property
    def required_column_names(self) -> List[str]:
        return [
            colexp.this.this
            for colexp in sqlglot.parse_one(self.select).find_all(expressions.Column)
        ]


@dataclass(frozen=True)
class Breakdown:
    name: str
    dimensions: Set[str]
    ways: List[Dimension]
    exclude_ways: Set[str] = field(default_factory=set)


@dataclass
class Report:
    name: str

    # In days
    period_length: int
    period_offset: int
    title: str
    tags: List[str]

    exclude_metrics: List[str] = field(default_factory=set)

    grow_orphan_at_max_depth: int = 0
    grow_segment_at_max_depth: int = 2  # Breakdown always has 1 more level


@dataclass(frozen=True)
class CpConfig:
    profile: Profile
    sources: List[Source]
    dimensions: List[Dimension]
    where: Optional[str]
    metrics: List[Metric] = field(default_factory=list)
    breakdowns: List[Breakdown] = field(default_factory=list)
    reports: List[Report] = field(default_factory=list)

    @cached_property
    def source_map(self) -> Dict[str, Source]:
        return {source.name: source for source in self.sources}

    @cached_property
    def dimension_map(self) -> Dict[str, Dimension]:
        return {dim.name: dim for dim in self.dimensions}

    @cached_property
    def breakdown_map(self) -> Dict[str, Breakdown]:
        return {breakdown.name: breakdown for breakdown in self.breakdowns}

    @cached_property
    def dimensions_per_source_name_map(self) -> Dict[str, List[Dimension]]:
        accum: Dict[str, List[Dimension]] = {}
        for dim in self.dimensions:
            if not dim.source:
                continue
            if dim.source not in accum:
                accum[dim.source] = []
            accum[dim.source].append(dim)
        return accum
