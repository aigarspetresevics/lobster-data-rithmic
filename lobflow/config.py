import yaml
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class SourceCfg:
    venue: str
    market: str = "futures"  # "spot" or "futures"


@dataclass
class CaptureCfg:
    depth_endpoint: str
    trade_endpoint: str
    snapshot_endpoint: str
    fut_depth_endpoint: str
    fut_trade_endpoint: str
    fut_snapshot_endpoint: str
    depth_limit: int
    reconnect_delay: int
    raw_dir: Path
    fut_force_order_endpoint: str = "wss://fstream.binance.com/ws/{sym_lower}@forceOrder"


@dataclass
class BarCfg:
    intervals: list[str]
    bucket_usd: float


@dataclass
class StorageCfg:
    provider: str
    bucket: str
    region: str
    endpoint: str
    csv_prefix: str = "csv/v1"
    parquet_prefix: str = "parquet/v1"


@dataclass
class CatalogCfg:
    type: str
    path: str = "./catalog.db"


@dataclass
class EnrichmentCfg:
    enabled: bool = False
    funding_dir: str = "./data/funding"
    oi_dir: str = "./data/oi"


@dataclass
class Settings:
    project_name: str
    symbols: list[str]
    source: SourceCfg
    capture: CaptureCfg
    bars: BarCfg
    storage: StorageCfg
    catalog: CatalogCfg
    enrichment: EnrichmentCfg = field(default_factory=EnrichmentCfg)
    logging: dict[str, str] = field(default_factory=dict)


def load_settings(path: str = "config.yaml") -> Settings:
    with open(path) as f:
        cfg = yaml.safe_load(f)

    capture = CaptureCfg(**cfg["capture"])
    capture.raw_dir = Path(capture.raw_dir).expanduser()
    capture.raw_dir.mkdir(parents=True, exist_ok=True)

    enrichment = EnrichmentCfg(**(cfg.get("enrichment") or {}))
    enrichment.funding_dir = str(Path(enrichment.funding_dir).expanduser())
    enrichment.oi_dir = str(Path(enrichment.oi_dir).expanduser())
    Path(enrichment.funding_dir).mkdir(parents=True, exist_ok=True)
    Path(enrichment.oi_dir).mkdir(parents=True, exist_ok=True)

    return Settings(
        project_name=cfg["project_name"],
        symbols=cfg["symbols"],
        source=SourceCfg(**cfg["source"]),
        capture=capture,
        bars=BarCfg(**cfg["bars"]),
        storage=StorageCfg(**cfg["storage"]),
        catalog=CatalogCfg(**cfg["catalog"]),
        enrichment=enrichment,
        logging=cfg.get("logging", {}),
    )


def url(template: str, sym: str, **kw) -> str:
    return template.format(sym=sym, sym_lower=sym.lower(), **kw)



