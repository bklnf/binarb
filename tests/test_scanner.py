from decimal import Decimal

from binarb.models import Book, Edge, Level, PairMeta
from binarb.scanner import (build_edges, discover_triangles, screen_top_of_book,
                            best_size_detailed, conservative_size_grid,
                            route_minimum_start, simulate,
                            top_of_book_opportunities)


def meta(symbol, base, quote, minimum="0.001", notional="1"):
    return PairMeta(symbol, base, quote, Decimal(minimum), Decimal(minimum), Decimal("100000"),
                    Decimal(notional), None, 8, Decimal(minimum), Decimal(minimum), Decimal("100000"))


def test_dense_graph_discovers_overlapping_directed_triangles():
    pairs = {"AUSD": meta("AUSD", "A", "USD"), "AB": meta("AB", "A", "B"),
             "BUSD": meta("BUSD", "B", "USD"), "CUSD": meta("CUSD", "C", "USD"),
             "CB": meta("CB", "C", "B")}
    tickers = {symbol: (Decimal("1"), Decimal("1.01")) for symbol in pairs}
    edges = build_edges(pairs, {symbol: Decimal("0") for symbol in pairs}, tickers)
    routes = discover_triangles(edges, "USD")
    assert ("USD", "A", "B", "USD") in routes
    assert ("USD", "C", "B", "USD") in routes


def test_ticker_math_compounds_fees_and_depth_is_strict():
    pairs = {"AUSD": meta("AUSD", "A", "USD"), "AB": meta("AB", "A", "B"),
             "BUSD": meta("BUSD", "B", "USD")}
    tickers = {"AUSD": (Decimal("9"), Decimal("10")), "AB": (Decimal("2"), Decimal("2.1")),
               "BUSD": (Decimal("6"), Decimal("6.1"))}
    fees = {symbol: Decimal(".001") for symbol in pairs}
    edges = build_edges(pairs, fees, tickers); routes = discover_triangles(edges, "USD")
    opportunity = top_of_book_opportunities(edges, routes, Decimal("10"), Decimal("-10000"))[0]
    expected = Decimal("10") / 10 * Decimal(".999") * 2 * Decimal(".999") * 6 * Decimal(".999")
    assert opportunity.end_amount == expected
    books = {"AUSD": Book((Level(Decimal("9"), Decimal("1")),), (Level(Decimal("10"), Decimal("1")),)),
             "AB": Book((Level(Decimal("2"), Decimal("1")),), (Level(Decimal("2.1"), Decimal("1")),)),
             "BUSD": Book((Level(Decimal("6"), Decimal("2")),), (Level(Decimal("6.1"), Decimal("2")),))}
    assert simulate(opportunity.edges, books, pairs, Decimal("11")) is None
    assert simulate(opportunity.edges, books, pairs, Decimal("10")) is not None


def test_screen_reports_best_signal_even_below_execution_threshold():
    pairs = {"AUSD": meta("AUSD", "A", "USD"), "AB": meta("AB", "A", "B"),
             "BUSD": meta("BUSD", "B", "USD")}
    tickers = {"AUSD": (Decimal("1"), Decimal("1.01")),
               "AB": (Decimal("1"), Decimal("1.01")),
               "BUSD": (Decimal("1"), Decimal("1.01"))}
    edges = build_edges(pairs, {symbol: Decimal(".001") for symbol in pairs}, tickers)
    opportunities, best_bps = screen_top_of_book(
        edges, discover_triangles(edges, "USD"), Decimal("10"), Decimal("100"),
    )
    assert opportunities == []
    assert best_bps is not None and best_bps < Decimal("100")


def test_blocked_symbol_is_excluded_from_graph():
    pairs = {"AUSD": meta("AUSD", "A", "USD")}
    edges = build_edges(pairs, {"AUSD": Decimal(".001")},
                        {"AUSD": (Decimal("1"), Decimal("1.01"))},
                        blocked_symbols={"AUSD"})
    assert edges == {}


def test_excluded_bridge_asset_is_excluded_from_graph():
    pairs = {"AIDR": meta("AIDR", "A", "IDR")}
    edges = build_edges(pairs, {"AIDR": Decimal(".001")},
                        {"AIDR": (Decimal("1"), Decimal("1.01"))},
                        excluded_assets={"IDR"})
    assert edges == {}


def test_conservative_grid_is_dense_with_full_balance_ceiling_and_exact_floor():
    grid = conservative_size_grid(Decimal("100"), Decimal("10"))
    assert len(grid) == 16
    assert grid[0] == Decimal("100")
    assert grid[-1] == Decimal("10")
    assert all(left > right for left, right in zip(grid, grid[1:]))
    assert conservative_size_grid(Decimal("10"), Decimal("10")) == [Decimal("10")]


def test_route_minimum_accounts_for_later_leg_notional():
    pairs = {"AUSD": meta("AUSD", "A", "USD", notional="1"),
             "AB": meta("AB", "A", "B", notional="5"),
             "BUSD": meta("BUSD", "B", "USD", notional="1")}
    edges = (Edge("USD", "A", "AUSD", "buy", Decimal("1"), Decimal(0)),
             Edge("A", "B", "AB", "sell", Decimal("1"), Decimal(0)),
             Edge("B", "USD", "BUSD", "sell", Decimal("1"), Decimal(0)))
    assert route_minimum_start(edges, pairs) == Decimal("5.05")


def test_detailed_sizing_exposes_pair_rule_rejection():
    pairs = {"AUSD": meta("AUSD", "A", "USD", notional="5"),
             "AB": meta("AB", "A", "B", notional="5"),
             "BUSD": meta("BUSD", "B", "USD", notional="5")}
    edges = (Edge("USD", "A", "AUSD", "buy", Decimal("1"), Decimal(0)),
             Edge("A", "B", "AB", "sell", Decimal("1"), Decimal(0)),
             Edge("B", "USD", "BUSD", "sell", Decimal("1"), Decimal(0)))
    books = {symbol: Book((Level(Decimal("1"), Decimal("100")),),
                          (Level(Decimal("1"), Decimal("100")),)) for symbol in pairs}
    candidate, diagnostic = best_size_detailed(
        edges, books, pairs, Decimal("1"), Decimal("4"), Decimal(0))
    assert candidate is None
    assert diagnostic["code"] == "PAIR_RULES"
