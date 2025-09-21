from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


class BessParams(BaseModel):
    """
    Parametry całego magazynu (BESS) – bez podziału na tory.
    """
    emax_mwh: float = Field(..., description="Pojemność całkowita [MWh]")
    c_rate_ch_mw: float = Field(..., description="Maks. moc ładowania [MW]")
    c_rate_dis_mw: float = Field(..., description="Maks. moc rozładowania [MW]")
    eta_ch: float = Field(..., description="Sprawność ładowania [0..1]")
    eta_dis: float = Field(..., description="Sprawność rozładowania [0..1]")
    self_discharge_per_h: float = Field(0.0, description="Samorozładowanie/h (ułamek)")
    soc_min_mwh: float = Field(0.0, description="Minimalny SOC [MWh]")
    soc_max_mwh: float = Field(..., description="Maksymalny SOC [MWh]")

    @property
    def emax(self) -> float:
        return self.emax_mwh


class TrackParams(BaseModel):
    """
    Parametry toru (OZE lub ARBI) po podziale pojemności.
    """
    emax_mwh: float = Field(..., description="Pojemność toru [MWh]")
    c_rate_ch_mw: float = Field(..., description="Maks. moc ładowania [MW]")
    c_rate_dis_mw: float = Field(..., description="Maks. moc rozładowania [MW]")
    eta_ch: float = Field(..., description="Sprawność ładowania [0..1]")
    eta_dis: float = Field(..., description="Sprawność rozładowania [0..1]")
    self_discharge_per_h: float = Field(0.0, description="Samorozładowanie/h (ułamek)")
    soc_min_mwh: float = Field(0.0, description="Minimalny SOC [MWh]")
    soc_max_mwh: float = Field(..., description="Maksymalny SOC [MWh]")
    soc_init_mwh: float = Field(0.0, description="Początkowy SOC [MWh]")


class Params(BaseModel):
    """
    Zestaw parametrów do przebiegu.
    """
    bess: BessParams
    share_oze: float
    oze: TrackParams
    arbi: TrackParams

    moc_umowna_mw: Optional[float] = None
    arbi_price_low: Optional[float] = None
    arbi_price_high: Optional[float] = None

    @property
    def emax(self) -> float:
        return self.bess.emax_mwh
