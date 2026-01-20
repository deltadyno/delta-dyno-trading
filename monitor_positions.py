from datetime import datetime, timedelta
import pytz
from alpaca.common.exceptions import APIError
import time
import math
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass
from alpaca.trading.requests import ClosePositionRequest
from misc import log_exception
from collections import defaultdict
from time import sleep
import traceback
from dataclasses import dataclass, asdict
from uuid import UUID
from enum import Enum


# Enum definitions for AssetClass, AssetExchange, and PositionSide
class AssetClass(Enum):
    US_OPTION = 'us_option'
    US_EQUITY = 'us_equity'

class AssetExchange(Enum):
    EMPTY = ''

class PositionSide(Enum):
    LONG = 'long'

@dataclass
class Position:
    asset_class: AssetClass
    asset_id: UUID
    asset_marginable: bool
    avg_entry_price: str
    change_today: str
    cost_basis: str
    current_price: str
    exchange: AssetExchange
    lastday_price: str
    market_value: str
    qty: str
    qty_available: str
    side: PositionSide
    symbol: str
    unrealized_intraday_pl: str
    unrealized_intraday_plpc: str
    unrealized_pl: str
    unrealized_plpc: str
    avg_entry_swap_rate: str = None  # Moved optional fields to the end
    swap_rate: str = None
    usd: str = None


def get_positions():
    position = Position(
        asset_class='us_option',
        asset_id=UUID('8c674c4e-6298-404c-807d-8014effb427a'),
        asset_marginable=True,
        avg_entry_price='0.16',
        change_today='-0.872',
        cost_basis='16',
        current_price='0.16',
        exchange=AssetExchange.EMPTY,
        lastday_price='1.25',
        market_value='16',
        qty='1',
        qty_available='15',
        side=PositionSide.LONG,
        symbol='SPY250630C00584000',
        unrealized_intraday_pl='0',
        unrealized_intraday_plpc='0',
        unrealized_pl='0',
        unrealized_plpc='-0.06'
    )
    return [position]  # Return list of Position objects

def get_positions2():
    position = Position(
        asset_class='us_option',
        asset_id=UUID('8c674c4e-6298-404c-807d-8014effb427a'),
        asset_marginable=True,
        avg_entry_price='0.16',
        change_today='-0.872',
        cost_basis='16',
        current_price='0.16',
        exchange=AssetExchange.EMPTY,
        lastday_price='1.25',
        market_value='16',
        qty='1',
        qty_available='15',
        side=PositionSide.LONG,
        symbol='SPY250630C00584000',
        unrealized_intraday_pl='0',
        unrealized_intraday_plpc='0',
        unrealized_pl='0',
        unrealized_plpc='-0.01'
    )
    return [position]  # Return list of Position objects

    
def get_positions3():
    position = Position(
        asset_class='us_option',
        asset_id=UUID('8c674c4e-6298-404c-807d-8014effb427a'),
        asset_marginable=True,
        avg_entry_price='0.16',
        change_today='-0.872',
        cost_basis='16',
        current_price='0.16',
        exchange=AssetExchange.EMPTY,
        lastday_price='1.25',
        market_value='16',
        qty='1',
        qty_available='15',
        side=PositionSide.LONG,
        symbol='SPY250630C00584000',
        unrealized_intraday_pl='0',
        unrealized_intraday_plpc='0',
        unrealized_pl='0',
        unrealized_plpc='-0.051'
    )
    return [position]  # Return list of Position objects

def monitor_positions_and_close(
        testcnt, 
        previous_unrealized_plpc, trailing_stop_loss_percentages, first_time_sales, expire_sale_seconds,
        closeorder, logger, close_all_at_min_profit, 
        trading_client, minimum_plpc, 
        ranges_list, stop_loss_values_list, stop_loss_quantity_sell_list, 
        default_stop_loss, close_all_trade_time_str, 
        tap_cnt_to_skip_hard_stop, cnt_to_skip_hard_stop,
        hardstop=0.25
    ):
    """
    Monitors active positions and closes them based on trailing stop loss and profit thresholds.
    """
    try:

        # Fetch all positions and initialize active symbols tracking
        positions = trading_client.get_all_positions()

        close_all_trade_time = datetime.strptime(str(close_all_trade_time_str), "%H:%M").time()
        
        """
        if testcnt == 1:
            positions = get_positions()
        if testcnt == 2:
            positions = get_positions2()
        if testcnt == 3:
            sleep(21)
            positions = get_positions3()
        """
        active_symbols = set()
        print("*************")
        logger.info("*************")
        
        for position in positions:
            try:

                logger.debug(f"Position Details json -> {str(position)}")

                # Check if position asset class is 'us_equity'
                if position.asset_class != 'us_option':
                    logger.debug(f"Skipping non-US equity position: {position.symbol}, position.asset_class: {position.asset_class}")
                    print(f"Skipping non-US equity position: {position.symbol}, position.asset_class: {position.asset_class}")
                    continue

                # Extract position data
                symbol = position.symbol
                unrealized_plpc = float(position.unrealized_plpc)
                current_price = float(position.current_price)

                qty = str(int(position.qty_available)) if hasattr(position, 'qty_available') else None

                # Skip if quantity is unavailable
                if qty is None or qty == "0":
                    print(f"Qty available is None/0. Skipping position -> {symbol}, qty {qty}")
                    logger.warning(f"Qty available is None/0. Skipping position -> {symbol}, qty {qty}")
                    continue
                
                active_symbols.add(symbol)
                print_position_status(symbol, qty, close_all_at_min_profit, unrealized_plpc, hardstop, trailing_stop_loss_percentages, minimum_plpc, logger)
                logger.debug(f"close_all_trade_time : {close_all_trade_time}, datetime.now().time() :{datetime.now().time()}")

                #Close all the postioins beyond this configured time
                # Check if unrealized loss exceeds hard stop
                if unrealized_plpc <= -hardstop and tap_cnt_to_skip_hard_stop < cnt_to_skip_hard_stop:
                    print(f"Skipping closing order. Hard stop skip {tap_cnt_to_skip_hard_stop} < Count configured ({cnt_to_skip_hard_stop})")
                    logger.info(f"Skipping closing order. Hard stop skip {tap_cnt_to_skip_hard_stop} < Count configured ({cnt_to_skip_hard_stop})")
                    tap_cnt_to_skip_hard_stop = tap_cnt_to_skip_hard_stop + 1
                    continue
                
                if unrealized_plpc <= -hardstop or unrealized_plpc >= close_all_at_min_profit or close_all_trade_time <= datetime.now().time():
                    print(f"Closing all - unrealizedPL {unrealized_plpc:.2%} either Hard stop {hardstop:.2%} has met, or max profit {close_all_at_min_profit:.2%} has met or close time {close_all_trade_time} is less than curren time {datetime.now().time()}.")
                    logger.info(f"Closing all - unrealizedPL {unrealized_plpc:.2%} either Hard stop {hardstop:.2%} has met, or max profit {close_all_at_min_profit:.2%} has met or close time {close_all_trade_time} is less than curren time {datetime.now().time()}.")
                    
                    if closeorder:    
                        close_position(symbol, qty, trailing_stop_loss_percentages, previous_unrealized_plpc, first_time_sales, True, trading_client, current_price, logger)
                        tap_cnt_to_skip_hard_stop = 0
                    else:
                        print("Skipping closing order. Close Order flag is disabled")
                        logger.info("Skipping closing order. Close flag Order is disabled")
                    continue

                # Initial trailing stop setup for profitable positions
                if unrealized_plpc >= minimum_plpc and symbol not in trailing_stop_loss_percentages:

                    to_sell_qty = determine_sell_quantity(unrealized_plpc, qty, symbol, first_time_sales, ranges_list, stop_loss_quantity_sell_list, expire_sale_seconds, logger)

                    if to_sell_qty > 0:
                        print(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")
                        logger.info(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")

                        full_close = False
                        if int(qty) - int(to_sell_qty) < 1:
                            full_close = True

                        if closeorder:
                            logger.debug(f"Attempting to close {symbol} with qty {to_sell_qty}.")
                            close_position(symbol, str(to_sell_qty), trailing_stop_loss_percentages, previous_unrealized_plpc, first_time_sales, full_close, trading_client, current_price, logger)
                        else:
                            print("Skipping close order. Close Order flag is disabled.")
                            logger.info("Skipping closing order. Close flag is disabled.")
                            
                        if full_close:
                            print("All quantity sold; trailing stop will not be set.")
                            logger.debug("All quantity sold; trailing stop will not be set.")
                            continue
                    else:
                        print(f"No quantity closed for symbol: {symbol}.")
                        logger.debug(f"No quantity closed for symbol: {symbol}")
                        
                    set_trailing_stop_loss(symbol, unrealized_plpc, trailing_stop_loss_percentages, previous_unrealized_plpc, ranges_list, stop_loss_values_list, default_stop_loss, logger)
                # Adjust trailing stop as profit rises
                elif symbol in trailing_stop_loss_percentages and unrealized_plpc > trailing_stop_loss_percentages[symbol]:

                    logger.debug(f"unrealized_plpc : {unrealized_plpc}, symbol : {symbol}, first_time_sales : {first_time_sales},previous_unrealized_plpc[symbol] : {previous_unrealized_plpc[symbol]}  ")

                    if unrealized_plpc > previous_unrealized_plpc[symbol]:

                        to_sell_qty = determine_sell_quantity(unrealized_plpc, qty, symbol, first_time_sales, ranges_list, stop_loss_quantity_sell_list, expire_sale_seconds, logger)

                        if to_sell_qty > 0:
                                print(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")
                                logger.info(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} profit")

                                full_close = False
                                if int(qty) - int(to_sell_qty) < 1:
                                    full_close = True

                                if closeorder:
                                    logger.debug(f"Attempting to close {symbol} with qty {to_sell_qty}.")
                                    close_position(symbol, str(to_sell_qty), trailing_stop_loss_percentages, previous_unrealized_plpc, first_time_sales, full_close, trading_client, current_price, logger)
                                else:
                                    print("Skipping close order. Close Order flag is disabled.")
                                    logger.info("Skipping closing order. Close flag is disabled.")
                                    
                                if full_close:
                                    print("All quantity sold; trailing stop will not be set.")
                                    logger.debug("All quantity sold; trailing stop will not be set.")
                                    continue
                        else:
                            print(f"No quantity closed for symbol: {symbol}.")
                            logger.debug(f"No quantity closed for symbol: {symbol}")                        
                            

                        # Comes here if not all set is sold
                        set_trailing_stop_loss(symbol, unrealized_plpc, trailing_stop_loss_percentages, previous_unrealized_plpc, ranges_list, stop_loss_values_list, default_stop_loss, logger)

                # Trigger trailing stop loss if profit falls below adjusted stop
                elif symbol in trailing_stop_loss_percentages and unrealized_plpc <= trailing_stop_loss_percentages[symbol]:
                    if closeorder:
                        close_position(symbol, qty, trailing_stop_loss_percentages, previous_unrealized_plpc, first_time_sales, True, trading_client, current_price, logger)
                    else:
                        print("Skipping closing order. Close Order flag is disabled")
                        logger.info("Skipping closing order. Close Order flag is disabled")
                elif unrealized_plpc < minimum_plpc:
                    
                    to_sell_qty = determine_sell_quantity(unrealized_plpc, qty, symbol, first_time_sales, ranges_list, stop_loss_quantity_sell_list, expire_sale_seconds, logger)
                    
                    if to_sell_qty > 0:
                        print(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} loss")
                        logger.info(f"Closing {to_sell_qty} units of {symbol} at {unrealized_plpc:.2%} loss")

                        full_close = False
                        if int(qty) - int(to_sell_qty) < 1:
                            full_close = True

                        if closeorder:
                            logger.debug(f"Attempting to close {symbol} with qty {to_sell_qty} for loss.")
                            close_position(symbol, str(to_sell_qty), trailing_stop_loss_percentages, previous_unrealized_plpc, first_time_sales, full_close, trading_client, current_price, logger)
                        else:
                            print("Skipping close order. Close Order flag is disabled.")
                            logger.info("Skipping closing order. Close flag is disabled.")
                            
                        if full_close:
                            print("All quantity sold; trailing stop will not be set.")
                            logger.debug("All quantity sold; trailing stop will not be set.")
                            continue
                    else:
                        print(f"No quantity closed for symbol: {symbol}.")
                        logger.debug(f"No quantity closed for symbol: {symbol}")

                else:
                    print(f"No action taken for {symbol}.")
                    logger.debug(f"No action taken for {symbol}.")
            except Exception as e:
                log_exception("Unexpected error. Moving to next position.", e, logger)
                continue

        # Periodic cleanup of symbols not in active positions
        cleanup_inactive_symbols(trailing_stop_loss_percentages, previous_unrealized_plpc, stop_loss_quantity_sell_list, active_symbols, logger)

    except Exception as e:
        log_exception("Unexpected error", e, logger)
    finally:
        print("*************")
        logger.info("*************")
        return trailing_stop_loss_percentages, tap_cnt_to_skip_hard_stop

def print_position_status(symbol, qty, close_all_at_min_profit, unrealized_plpc, hardstop, trailing_stop_loss_percentages, minimum_plpc, logger):
    """
    Logs the status of a position including quantity, unrealized profit/loss, and stop levels.
    """
    trailing_stop = trailing_stop_loss_percentages.get(symbol)
    if trailing_stop is None:
        print(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: {minimum_plpc:.2%}, trailing_stop: NA, close_all_at_min_profit: {close_all_at_min_profit:.2%} ")
        logger.info(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: {minimum_plpc:.2%}, trailing_stop: NA, close_all_at_min_profit: {close_all_at_min_profit:.2%}")
    else:
        print(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: NA, trailing_stop: {trailing_stop:.2%}, close_all_at_min_profit: {close_all_at_min_profit:.2%} ")
        logger.info(f"Symbol: {symbol}, qty: {qty}, unrealized_PL: {unrealized_plpc:.2%}, hardstop: {hardstop:.2%}, profitprcntageforstoploss: NA, trailing_stop: {trailing_stop:.2%}, close_all_at_min_profit: {close_all_at_min_profit:.2%}")

def close_position(symbol, qty, trailing_stop_loss_percentages, previous_unrealized_plpc, first_time_sales, full_close, trading_client, current_price, logger):
    """
    Closes a position and removes it from tracking dictionaries.
    """
    logger.debug(f"Closing position : {symbol} for qty : {qty}")
    close_request = ClosePositionRequest(side=OrderSide.SELL, qty=qty)
    response = trading_client.close_position(symbol, close_options=close_request)
    print(f"Closed position for {symbol}")
    logger.info(f"Update position - Symbol: {symbol}, qty: {qty}, order_type: market, side: sell, status: filled, price: {current_price}")
    logger.debug(f"Response : {response}")
    if symbol in trailing_stop_loss_percentages:
        del trailing_stop_loss_percentages[symbol]
        del previous_unrealized_plpc[symbol]
        if full_close:
            del first_time_sales[symbol]

def determine_sell_quantity(unrealized_plpc, qty, symbol, first_time_sales, profit_percent_ranges, sell_quantity_percentages, expire_sale_seconds, logger):
    logger.debug(f"Determining sell quantity for {symbol} with unrealized profit: {unrealized_plpc:.2%}")
    logger.debug(f"Profit ranges: {profit_percent_ranges}: to_sell_qty : {qty}")
    print(f"[DEBUG] determine_sell_quantity called for symbol={symbol}, unrealized_plpc={unrealized_plpc}, qty={qty}")

    now_utc = datetime.utcnow()
    #print(f"[DEBUG] Current UTC time: {now_utc}")

    for i, (low, high) in enumerate(profit_percent_ranges):
        #print(f"[DEBUG] Checking range index={i}, low={low}, high={high}, unrealized_plpc*100={unrealized_plpc*100}")
        if low <= unrealized_plpc * 100 < high:
            #print(f"[DEBUG] {symbol} falls in profit range {low}-{high}% (index {i})")

            if symbol not in first_time_sales:
                first_time_sales[symbol] = {}
                print(f"[DEBUG] {symbol} not in first_time_sales, initializing.")

            # Check if this range was already handled
            if i in first_time_sales[symbol]:
                recorded_utc = first_time_sales[symbol][i]
                #print(f"[DEBUG] {symbol} already sold in this range at {recorded_utc}")
                if now_utc - recorded_utc > timedelta(seconds=expire_sale_seconds):
                    print(f"{symbol} in range {low}-{high}% was last recorded over {expire_sale_seconds}s ago. Resetting.")
                    logger.debug(f"{symbol} in range {low}-{high}% was last recorded over {expire_sale_seconds}s ago. Resetting.")
                    del first_time_sales[symbol][i]
                else:
                    print(f"Range {low}-{high}% already sold for {symbol} recently. No further action.")
                    logger.debug(f"Range {low}-{high}% already sold for {symbol} recently. No further action.")
                    return 0

            # Now safe to set new timestamp and sell
            first_time_sales[symbol][i] = now_utc
            #print(f"[DEBUG] Setting first_time_sales[{symbol}][{i}] = {now_utc}")

            sell_percent = sell_quantity_percentages[i]
            #print(f"[DEBUG] Sell percent for index {i}: {sell_percent}")
            sell_qty = max(math.floor(int(qty) * sell_percent), 1) if sell_percent > 0 else 0
            #print(f"[DEBUG] Calculated sell_qty: {sell_qty} (qty={qty}, sell_percent={sell_percent})")

            logger.debug(f"Range {low}-{high}% triggered for {symbol}. Selling {sell_qty} units.")
            return sell_qty

    print(f"[DEBUG] No matching profit range found for {symbol}. Returning 0.")
    return 0

# Function to get trailing stop loss value based on unrealized_plpc
def get_trailing_stop_loss(unrealized_plpc, ranges_list, stop_loss_values_list, default_stop_loss):
    for i, (lower, upper) in enumerate(ranges_list):
        if lower <= unrealized_plpc * 100 < upper:
            return stop_loss_values_list[i]
    return default_stop_loss  # Return None if no match is found

def set_trailing_stop_loss(
    symbol, unrealized_plpc, trailing_stop_loss_percentages,
    previous_unrealized_plpc, ranges_list, stop_loss_values_list,
    default_stop_loss, logger
):
    """
    Sets or updates the trailing stop loss based on unrealized profit/loss.
    """
    
    # Print all argument values for easier debugging and tracking
    #print(f"Arguments received - symbol: {symbol}")
    #print(f"unrealized_plpc: {unrealized_plpc}")
    #print(f"ranges_list: {ranges_list}")
    #print(f"stop_loss_values_list: {stop_loss_values_list}")
    #print(f"default_stop_loss: {default_stop_loss}")
    #print(f"minimum_plpc: {minimum_plpc}")
    #print(f" getTrailingStopLoss: {get_trailing_stop_loss(unrealized_plpc, ranges_list, stop_loss_values_list, default_stop_loss)}")
    
    trailing_stop = unrealized_plpc - get_trailing_stop_loss( unrealized_plpc, ranges_list, stop_loss_values_list, default_stop_loss)
    
    # Update stop loss data
    trailing_stop_loss_percentages[symbol] = trailing_stop
    previous_unrealized_plpc[symbol] = unrealized_plpc

    # Print updated trailing stop for confirmation
    print(f"Setting trailing stop loss for {symbol} at {trailing_stop:.2%}")
    logger.info(f"Setting trailing stop loss for {symbol} at {trailing_stop:.2%}")

def cleanup_inactive_symbols(trailing_stop_loss_percentages, previous_unrealized_plpc,first_time_sales, active_symbols, logger):
    """
    Removes symbols from trailing stop tracking if they are no longer active.
    """
    #logger.debug(f"Cleanup trailing_stop_loss_percentages Dict: {dict(trailing_stop_loss_percentages)}")  # Convert to regular dict for clearer printing
    #logger.debug(f"Cleanup previous_unrealized_plpc Dict: {dict(previous_unrealized_plpc)}")  # Convert to regular dict for clearer printing
    #logger.debug(f"Cleanup first_time_sales Dict: {dict(first_time_sales)}")  # Convert to regular dict for clearer printing
    #logger.debug(f"Cleanup active_symbols : {active_symbols}")  # Convert to regular dict for clearer printing
    for symbol in list(trailing_stop_loss_percentages.keys()):
        if symbol not in active_symbols:
            del trailing_stop_loss_percentages[symbol]

    for symbol in list(previous_unrealized_plpc.keys()):
        if symbol not in active_symbols:
            del previous_unrealized_plpc[symbol]

    for symbol in list(first_time_sales):
        if symbol not in active_symbols:
            first_time_sales.remove(symbol)

                
