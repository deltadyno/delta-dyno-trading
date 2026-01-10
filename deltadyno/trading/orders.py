"""
Order placement utilities for the DeltaDyno trading system.

This module provides functions for placing various types of orders
through the Alpaca API.
"""

import traceback
from typing import Optional

from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce


def place_order(
    trading_client,
    symbol: str,
    qty: int,
    stop_price: float,
    limit_price: float,
    is_limit_order: bool,
    logger,
    side: OrderSide = OrderSide.BUY,
    time_in_force: TimeInForce = TimeInForce.DAY
) -> Optional[dict]:
    """
    Place an order through Alpaca.

    Args:
        trading_client: Alpaca TradingClient instance
        symbol: Symbol to trade (e.g., 'SPY241218C00604000')
        qty: Quantity of contracts to order
        stop_price: Stop price (not used for market orders)
        limit_price: Limit price for limit orders, current price for market orders
        is_limit_order: True for limit order, False for market order
        logger: Logger instance
        side: Order side (BUY or SELL), defaults to BUY
        time_in_force: Time in force (DAY, GTC, etc.), defaults to DAY

    Returns:
        Order response dictionary, or None on error
    """
    if qty <= 0:
        logger.warning(f"Invalid order quantity {qty}. Skipping order placement.")
        return None

    try:
        if is_limit_order:
            order_request = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=time_in_force,
                limit_price=limit_price
            )
            logger.info(
                f"Placing limit order - Symbol: {symbol}, Qty: {qty}, "
                f"Side: {side.value}, Limit: ${limit_price:.2f}"
            )
        else:
            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=time_in_force
            )
            logger.info(
                f"Placing market order - Symbol: {symbol}, Qty: {qty}, "
                f"Side: {side.value}, Current Price: ${limit_price:.2f}"
            )

        order = trading_client.submit_order(order_request)

        order_id = order.id if hasattr(order, 'id') else order.get('id', 'unknown')
        logger.info(f"Order submitted successfully - ID: {order_id}")

        return order

    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(
            f"Error placing order for {symbol}: {e}\n"
            f"Traceback:\n{error_traceback}"
        )
        print(f"Error placing order for {symbol}: {e}")
        return None


def place_market_order(
    trading_client,
    symbol: str,
    qty: int,
    side: OrderSide,
    logger,
    time_in_force: TimeInForce = TimeInForce.DAY
) -> Optional[dict]:
    """
    Place a market order.

    Args:
        trading_client: Alpaca TradingClient instance
        symbol: Symbol to trade
        qty: Quantity to order
        side: Order side (BUY or SELL)
        logger: Logger instance
        time_in_force: Time in force, defaults to DAY

    Returns:
        Order response, or None on error
    """
    return place_order(
        trading_client=trading_client,
        symbol=symbol,
        qty=qty,
        stop_price=0.0,
        limit_price=0.0,
        is_limit_order=False,
        logger=logger,
        side=side,
        time_in_force=time_in_force
    )


def place_limit_order(
    trading_client,
    symbol: str,
    qty: int,
    limit_price: float,
    side: OrderSide,
    logger,
    time_in_force: TimeInForce = TimeInForce.DAY
) -> Optional[dict]:
    """
    Place a limit order.

    Args:
        trading_client: Alpaca TradingClient instance
        symbol: Symbol to trade
        qty: Quantity to order
        limit_price: Limit price
        side: Order side (BUY or SELL)
        logger: Logger instance
        time_in_force: Time in force, defaults to DAY

    Returns:
        Order response, or None on error
    """
    return place_order(
        trading_client=trading_client,
        symbol=symbol,
        qty=qty,
        stop_price=0.0,
        limit_price=limit_price,
        is_limit_order=True,
        logger=logger,
        side=side,
        time_in_force=time_in_force
    )

