SYMBOL = "SPY"
CURRENCY = "PLN"

# names of the database tables
SECURITY_TABLE = f'src_{SYMBOL.lower()}_price_usd'
CURRENCY_TABLE = f'src_usd_{CURRENCY.lower()}'
COMPARISON_TABLE = f'prd_{SYMBOL.lower()}_price_{CURRENCY.lower()}'

# a tuple of colors used in the price report
COLORS = ('#0080FF', '#FF8000', '#00FF00', '#0000FF', '#FF0000', '#007700')

# a tuple of window sizes of simple moving averages used in the price report
SMA = (20, 90)
