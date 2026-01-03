from tradedesk_dukascopy.cli import build_parser


def test_parser_accepts_required_args():
    p = build_parser()
    args = p.parse_args(
        [
            "--symbol",
            "EURUSD",
            "--from",
            "2024-01-01",
            "--to",
            "2024-01-02",
            "--out",
            "out.csv",
        ]
    )
    assert args.symbol == "EURUSD"
    assert args.format == "candles"
    assert args.price_divisor == 1.0
