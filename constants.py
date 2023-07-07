# A security of which the price will be presented in the report
SYMBOL = "SPY"

# A currency (besides USD) in which the price of the security, defined by the SYMBOL variable, will be calculated and
# presented in the report
CURRENCY = "PLN"

# names of database tables
SECURITY_TABLE = f'src_{SYMBOL.lower()}_price_usd'
CURRENCY_TABLE = f'src_usd_{CURRENCY.lower()}'
COMPARISON_TABLE = f'prd_{SYMBOL.lower()}_price_{CURRENCY.lower()}'

# a tuple of colors used in the price report
COLORS = ('#0080FF', '#FF8000', '#00FF00', '#0000FF', '#FF0000', '#007700')

# a tuple of window sizes of simple moving averages used in the price report
SMA = (20, 90)
