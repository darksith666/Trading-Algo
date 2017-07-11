def initialize(context):
    context.security = sid(24)

    #Schedule a 'rebalance' method to run once a day
    schedule_function(rebalance, date_rule = date_rules.every_day())

def rebalance(context, data):
    #To make market decisions, calculate the stock's moving average for the last 5 days.

    #Get the price history for the last 5 days.
    price_history = data.history(
        context.security,
        fields = 'price',
        bar_count = 5,
        frequency = '1d'
    )

    #Take an average of those 5 days.
    average_price = price_history.mean()

    #Obtain the stock's current price.
    current_price = data.current(context.security, 'price')

    #If the stock is currently listed on a major exchange.
    if data.can_trade(context.security):
        if current_price > (1.01 * average_price):
            #Place the buy order (positive means buy, negative means sell).
            open_orders = get_open_orders()
            if context.security not in open_orders:
                order_target_percent(context.security, 1)
                log.info("Buying %s" % (context.security.symbol))
        elif current_price < average_price:
            #Sell all shares by setting the target position to zero.
            order_target_percent(context.security, 0)
            log.info("Selling %s" % (context.security.symbol))

        #Record stock's current price and the average price over the last five days.
        record(current_price = current_price, average_price = average_price)
