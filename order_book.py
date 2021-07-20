from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


def process_order(order):
    # 1. Insert this order into the order book
    if isinstance(order, dict):
        order_obj = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'],
                          buy_currency=order['buy_currency'], sell_currency=order['sell_currency'],
                          buy_amount=order['buy_amount'], sell_amount=order['sell_amount'])
    else:
        order_obj = order
    session.add(order_obj)
    session.commit()

    # 2. Check if there are any existing orders that match
    matched = False
    orders = [order for order in session.query(Order).filter(Order.filled == None).all()]
    for existing_oder in orders:
        if existing_oder.buy_currency == order_obj.sell_currency and \
                existing_oder.sell_currency == order_obj.buy_currency:
            if existing_oder.sell_amount / existing_oder.buy_amount >= order_obj.buy_amount / order_obj.sell_amount:
                # If a match is found
                matched = True
                existing_oder.filled = datetime.now()
                order_obj.filled = datetime.now()
                existing_oder.counterparty_id = order_obj.id
                order_obj.counterparty_id = existing_oder.id
                session.commit()
                break

    if matched:
        # If one of the orders is not completely filled
        if existing_oder.sell_amount < order_obj.buy_amount:
            new_order_obj = Order(sender_pk=order_obj.sender_pk, receiver_pk=order_obj.receiver_pk,
                                  buy_currency=order_obj.buy_currency, sell_currency=order_obj.sell_currency,
                                  buy_amount=order_obj.buy_amount - existing_oder.sell_amount,
                                  sell_amount=order_obj.sell_amount - existing_oder.buy_amount,
                                  creator_id=order_obj.id)

        elif order_obj.sell_amount < existing_oder.buy_amount:
            new_order_obj = Order(sender_pk=existing_oder.sender_pk, receiver_pk=existing_oder.receiver_pk,
                                  buy_currency=existing_oder.buy_currency,
                                  sell_currency=existing_oder.sell_currency,
                                  buy_amount=existing_oder.buy_amount - order_obj.sell_amount,
                                  sell_amount=existing_oder.sell_amount - order_obj.buy_amount,
                                  creator_id=existing_oder.id)
        else:
            pass
        session.add(new_order_obj)
        session.commit()
        process_order(new_order_obj)
