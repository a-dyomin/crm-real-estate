from dataclasses import dataclass

from app.models.enums import SourceChannel
from app.services.dedup import make_fingerprint


@dataclass
class Payload:
    source_channel: SourceChannel
    source_external_id: str | None
    normalized_address: str | None
    area_sqm: float | None
    price_rub: float | None
    contact_phone: str | None
    contact_email: str | None


def test_make_fingerprint_is_stable_for_phone_format() -> None:
    left = Payload(
        source_channel=SourceChannel.cian,
        source_external_id=None,
        normalized_address="Ижевск, Ленина 1",
        area_sqm=100.0,
        price_rub=1000000,
        contact_phone="+7 (900) 111-22-33",
        contact_email=None,
    )
    right = Payload(
        source_channel=SourceChannel.cian,
        source_external_id=None,
        normalized_address="ижевск,  ленина 1",
        area_sqm=100.4,
        price_rub=1000400,
        contact_phone="8 900 111 22 33",
        contact_email=None,
    )

    assert make_fingerprint(left) == make_fingerprint(right)

