import pytest

from fakes import fixture
from keel.hl.coins import ASSET_OFFSET, is_outcome_coin, parse_coin, side_coin
from keel.hl.metadata import Universe, classify, parse_description, parse_outcome_meta, summarize
from keel.units import TICK_PX7, UnitError, parse_duration, px7, sz6, uusdc


def test_px7_exact_and_tick():
    assert px7("0.31834") == 3_183_400
    assert px7("0.00001") == TICK_PX7
    assert px7("1") == 10_000_000
    assert sz6("616.0") == 616_000_000
    assert uusdc("10") == 10_000_000


def test_px7_refuses_excess_precision_and_garbage():
    with pytest.raises(UnitError):
        px7("0.123456789")
    with pytest.raises(UnitError):
        px7("abc")
    with pytest.raises(UnitError):
        px7("NaN")


def test_duration_requires_units():
    assert parse_duration("60s") == 60
    assert parse_duration("15m") == 900
    assert parse_duration("500ms") == 0.5
    with pytest.raises(UnitError):
        parse_duration(60)
    with pytest.raises(UnitError):
        parse_duration("60")


def test_coin_encoding_matches_venue():
    c = side_coin(43639, 0)
    assert c.coin == "#436390"
    assert c.asset_id == ASSET_OFFSET + 436390
    assert c.balance_token == "+436390"
    assert side_coin(43639, 1).coin == "#436391"
    assert parse_coin("#436391").outcome == 43639 and parse_coin("#436391").side == 1
    assert not is_outcome_coin("BTC")
    with pytest.raises(ValueError):
        parse_coin("#436392")
    with pytest.raises(ValueError):
        side_coin(1, 2)


def test_description_parsing_keeps_colons_in_values():
    d = parse_description("perp:xyz:SPCX|priceDescription:1-minute candle|seconds:300")
    assert d == {"perp": "xyz:SPCX", "priceDescription": "1-minute candle", "seconds": "300"}
    assert parse_description("other") == {"_text": "other"}
    assert parse_description("") == {}


def test_parse_real_outcome_meta():
    meta = parse_outcome_meta(fixture("outcome_meta_2026-11-23.json"))
    assert len(meta.outcomes) == 8
    viking = meta.outcomes[43686]
    assert viking.venue == "out" and viking.category == "sports"
    assert viking.question == 690 and viking.role == "named"
    assert meta.outcomes[43683].role == "fallback"
    native = meta.outcomes[44594]
    assert native.venue == "native" and native.category == "price" and native.fee_scale is None
    assert meta.outcomes[41402].category == "price" and meta.outcomes[41402].side_names == ("template:Yes", "template:No")
    q = meta.questions[690]
    assert q.fields["sport"] == "Soccer" and q.fields["scheduledStart"] == "20261125-2000"
    s = summarize(meta.outcomes.values())
    assert s["by_venue"] == {"native": 1, "out": 5, "skew": 1, "txyz": 1}


def test_universe_tracks_instances_removals_and_edits():
    doc = fixture("outcome_meta_2026-11-23.json")
    u = Universe()
    ch = u.apply(parse_outcome_meta(doc), 1000)
    assert len(ch.added) == 8 and not ch.removed
    inst = u.live[43639]
    assert inst.instance_id == "43639@1000"
    # edit description, remove one outcome, settle a named outcome
    doc2 = fixture("outcome_meta_2026-11-23.json")
    doc2["outcomes"] = [o for o in doc2["outcomes"] if o["outcome"] != 43639]
    for o in doc2["outcomes"]:
        if o["outcome"] == 43686:
            o["description"] = "participant:Viking FK"
    doc2["questions"][0]["settledNamedOutcomes"] = [43685]
    ch2 = u.apply(parse_outcome_meta(doc2), 2000)
    assert [i.outcome.outcome for i in ch2.removed] == [43639]
    assert ch2.removed[0].removed_ns == 2000
    assert [(i.outcome.outcome, p.description) for i, p in ch2.changed] == [(43686, "participant:Viking")]
    assert ch2.newly_settled_named == [(690, 43685)]
    # id reuse after removal creates a new instance
    ch3 = u.apply(parse_outcome_meta(doc), 3000)
    assert [i.instance_id for i in ch3.added] == ["43639@3000"]


def test_classify():
    assert classify("template:sportsContestResult", {}) == "sports"
    assert classify("Recurring", {"class": "priceBinary"}) == "price"
    assert classify("template:macro", {}) == "other"
