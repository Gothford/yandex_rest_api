from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, jsonify, Response
from collections import Counter
import sqlalchemy.exc as exc
from sqlite3 import IntegrityError as sqIntegrityError
from sqlite3 import OperationalError as sqOperationalError
from datetime import datetime
import json

WEIGHTS = {'foot': 10,
           'bike': 15,
           'car': 50}

COURIER_KEYS_CHECKLIST = ['courier_id', 'courier_type', 'regions', 'working_hours']
ORDER_KEYS_CHECKLIST = ['order_id', 'weight', 'region', 'delivery_hours']
COMPLETE_ORDERS_KEYS_CHECKLIST = ['courier_id', 'order_id', 'complete_time']

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db?check_same_thread=False'
db = SQLAlchemy(app)


class Couriers(db.Model):
    courier_id = db.Column(db.Integer, primary_key=True)
    courier_type = db.Column(db.String(10), nullable=False)
    max_weight = db.Column(db.Integer, nullable=False)

    def __repr__(self):
        return '<Courier %r>' % self.id


class Orders(db.Model):
    order_id = db.Column(db.Integer, primary_key=True)
    weight = db.Column(db.Float, nullable=False)
    region = db.Column(db.Integer, nullable=False)
    delivery_hour_start = db.Column(db.Time, nullable=False)
    delivery_hour_end = db.Column(db.Time, nullable=False)
    assigned_courier_id = db.Column(db.Integer)
    assigned_time = db.Column(db.DateTime)
    execution_time = db.Column(db.String(50))

    def __repr__(self):
        return '<Order %r>' % self.id


class Couriers_regions(db.Model):
    courier_id = db.Column(db.Integer, primary_key=True)
    region = db.Column(db.Integer, primary_key=True)

    def __repr__(self):
        return '<Couriers_regions %r>' % self.id


class Couriers_work_hours(db.Model):
    courier_id = db.Column(db.Integer, primary_key=True)
    start_time = db.Column(db.Time, primary_key=True)
    end_time = db.Column(db.Time, primary_key=True)

    def __repr__(self):
        return '<Couriers_regions %r>' % self.id


@app.route('/couriers', methods=['POST'])
def create_courier():

    error_data = {"validation_error": {"couriers": []}}
    success_ids = {"couriers": []}

    for i in range(len(request.json['data'])):
        try:
            courier_data = request.json['data'][i]

            if Counter(courier_data.keys()) != Counter(COURIER_KEYS_CHECKLIST):
                raise KeyError

            courier_id = courier_data['courier_id']
            courier_type = courier_data['courier_type']
            max_weight = WEIGHTS.get(courier_data['courier_type'], None)
            regions = courier_data['regions']
            working_hours = courier_data['working_hours']

            assert isinstance(courier_id, int)
            assert courier_type in WEIGHTS.keys()
            assert max_weight is not None
            assert isinstance(regions, list)
            assert isinstance(working_hours, list)
            assert regions != [] and working_hours != []

            select_id = f'select * from couriers where courier_id = {courier_id}'
            res = db.engine.execute(select_id).first()

            if res is not None:
                raise KeyError

            courier = Couriers(courier_id=courier_id,
                              courier_type=courier_type,
                              max_weight=max_weight)

            db.session.add(courier)

            for reg in regions:
                courier_region = Couriers_regions(courier_id=courier_id,
                                                  region=reg)
                db.session.add(courier_region)

            for wh in working_hours:
                start_time, end_time = wh.split('-')

                if len(start_time.split(':')) >= 3:
                    start_time = start_time[:-3]
                if len(end_time.split(':')) >= 3:
                    end_time = end_time[:-3]

                start_time = datetime.strptime(start_time, '%H:%M').time()
                end_time = datetime.strptime(end_time, '%H:%M').time()
                courier_wh = Couriers_work_hours(courier_id=courier_id,
                                                 start_time=start_time,
                                                 end_time=end_time)
                db.session.add(courier_wh)

            success_ids['couriers'].append({'id': request.json['data'][i]['courier_id']})

        except (KeyError, AssertionError) :
            error_data['validation_error']['couriers'].append({'id': request.json['data'][i]['courier_id']})
            continue

    if len(error_data['validation_error']['couriers']) != 0:
        return Response(f'HTTP 400 Bad Request\n{json.dumps(error_data)}', status=400)
    else:
        db.session.commit()
        return Response(f'HTTP 201 Created\n{json.dumps(success_ids)}', status=201)


@app.route('/couriers/<courier_id>', methods=['PATCH'])
def patch_courier(courier_id):

    data = request.json

    def db_updater(data_to_update, courier_id):
        for order_id in data_to_update:
            update_query = f'update orders set assigned_time = null where assigned_courier_id = {courier_id} ' \
                f' and execution_time is null and order_id = {order_id[0]}'
            db.engine.execute(update_query)
            update_query = f'update orders set assigned_courier_id = null where assigned_courier_id = {courier_id} ' \
                f' and execution_time is null and order_id = {order_id[0]}'
            db.engine.execute(update_query)

    for k, v in data.items():
        if k == 'regions':
            table = [f'delete from couriers_regions where courier_id = {courier_id}',
                          'insert into couriers_regions values ({0}, {1})']

            delete_query = table[0]
            db.engine.execute(delete_query)
            for i in v:
                db.engine.execute(table[1].format(courier_id, i))

            cleaner_query = f'select o.order_id from orders as o' \
                f' join couriers_regions as c_reg on c_reg.courier_id = {courier_id}' \
                f' and c_reg.region != o.region where o.assigned_courier_id == {courier_id}'

            data_to_update = db.engine.execute(cleaner_query).all()

            if data_to_update != []:
                db_updater(data_to_update, courier_id)

        elif k == 'courier_type':
            tables = [f'update couriers set courier_type = "{v}" where courier_id = {courier_id}',
                               f'update couriers set max_weight = {WEIGHTS[v]} where courier_id = {courier_id}']
            for query in tables:
                db.engine.execute(query)

            cleaner_query = f'select o.order_id from orders as o' \
                f' join couriers as c on c.courier_id = {courier_id}' \
                f' and c.max_weight < o.weight where o.assigned_courier_id == {courier_id}'
            data_to_update = db.engine.execute(cleaner_query).all()

            if data_to_update != []:
                db_updater(data_to_update, courier_id)

        elif k == 'working_hours':
            try:
                assert isinstance(v, list)
            except AssertionError:
                return Response(f'HTTP 400 Bad Request\n', status=400)

            table = [f'delete from couriers_work_hours where courier_id = {courier_id}',
                                'insert into couriers_work_hours (courier_id, start_time, end_time) values ({0}, "{1}", "{2}")']
            delete_query = table[0]
            db.engine.execute(delete_query)
            for timer in v:
                start_time, end_time = timer.split('-')

                if len(start_time.split(':')) >= 3:
                    start_time = start_time[:-3]
                if len(end_time.split(':')) >= 3:
                    end_time = end_time[:-3]

                start_time = datetime.strptime(start_time, '%H:%M').time()
                end_time = datetime.strptime(end_time, '%H:%M').time()
                db.engine.execute(table[1].format(courier_id, start_time, end_time))

            cleaner_query = f'select o.order_id from orders as o' \
                f' join couriers_work_hours as c_wh on c_wh.start_time >= o.delivery_hour_end' \
                f' or c_wh.end_time <= o.delivery_hour_start where o.assigned_courier_id == {courier_id}'

            data_to_update = db.engine.execute(cleaner_query).all()

            if data_to_update != []:
                db_updater(data_to_update, courier_id)

        else:
            return Response(f'HTTP 400 Bad Request\n', status=400)

    db.session.commit()

    courier = Couriers.query.filter_by(courier_id=courier_id).first()
    courier_reg = Couriers_regions.query.filter_by(courier_id=courier_id).all()
    courier_wh = Couriers_work_hours.query.filter_by(courier_id=courier_id).all()

    courier_data = {"courier_id": courier.courier_id,
                    "courier_type": courier.courier_type,
                    "regions": [i.region for i in courier_reg],
                    "working_hours": [f'{i.start_time.isoformat()}-{i.end_time.isoformat()}' for i in courier_wh]}

    return Response(f'HTTP 200 OK\n{json.dumps(courier_data)}', status=200)


@app.route('/orders', methods=['POST'])
def create_orders():

    error_data = {"validation_error": {"orders": []}}

    success_ids = {"orders": []}

    for i in range(len(request.json['data'])):
        try:
            order_data = request.json['data'][i]

            if Counter(order_data.keys()) != Counter(ORDER_KEYS_CHECKLIST):
                raise KeyError

            order_id = order_data['order_id']
            weight = round(order_data['weight'], 2)
            region = order_data['region']
            delivery_hour_start, delivery_hour_end = order_data['delivery_hours'][0].split('-')
            delivery_hour_start = datetime.strptime(delivery_hour_start, '%H:%M').time()
            delivery_hour_end = datetime.strptime(delivery_hour_end, '%H:%M').time()

            assert isinstance(order_id, int)
            assert 0.01 <= weight <= 50
            assert isinstance(region, int)

            select_id = f'select * from orders where order_id = {order_id}'
            res = db.engine.execute(select_id).first()

            if res is not None:
                raise KeyError

            order = Orders(order_id=order_id,
                           weight=weight,
                           region=region,
                           delivery_hour_start=delivery_hour_start,
                           delivery_hour_end=delivery_hour_end)

            db.session.add(order)
            success_ids['orders'].append({'id': order_id})

        except (KeyError, AssertionError, exc.OperationalError, exc.IntegrityError, sqIntegrityError, sqOperationalError) as e:
            error_data['validation_error']['orders'].append({'id': request.json['data'][i]['order_id']})
            continue

    if len(error_data['validation_error']['orders']) != 0:
        return Response(f'HTTP 400 Bad Request\n{json.dumps(error_data)}', status=400)
    else:
        db.session.commit()
        return Response(f'HTTP 201 Created\n{json.dumps(success_ids)}', status=201)


@app.route('/orders/complete', methods=['POST'])
def create_complete():
    # todo проверка валидности
    try:
        complete_order_data = request.json
        if Counter(complete_order_data.keys()) != Counter(COMPLETE_ORDERS_KEYS_CHECKLIST):
            raise KeyError
        courier_id = complete_order_data['courier_id']
        order_id = complete_order_data['order_id']
        complete_time = complete_order_data['complete_time']

        select_query = f'select * from orders where assigned_courier_id = {courier_id} and order_id = {order_id} and execution_time is null'
        result = db.engine.execute(select_query).all()

        if isinstance(result, list) and len(result)!=0:

            update_query = f"update orders" \
                f" set execution_time = '{complete_time}'" \
                f" where order_id = {order_id}"
            exec_query = db.engine.execute(update_query)

        else:
            raise KeyError

    except (exc.OperationalError, exc.IntegrityError, sqIntegrityError,
            sqOperationalError, KeyError, exc.SQLAlchemyError,
            exc.DatabaseError):  # todo add IntegrityError

        return Response(f'HTTP 400 Bad Request\n', status=400)

    data = {'order_id': order_id}
    db.session.commit()
    return Response(f'HTTP 200 OK\n{json.dumps(data)}', status=200)


@app.route('/courier/<courier_id>', methods=['GET'])
def get_courier(courier_id):

    courier = Couriers.query.filter_by(courier_id=courier_id).first()
    courier_wh = Couriers_work_hours.query.filter_by(courier_id=courier_id).all()
    courier_reg = Couriers_regions.query.filter_by(courier_id=courier_id).all()

    courier_data = {"courier_id": courier.courier_id,
                    "courier_type": courier.courier_type,
                    "regions": [i.region for i in courier_reg],
                    "working_hours": [f'{i.start_time.isoformat()}-{i.end_time.isoformat()}' for i in courier_wh]
                    #"rating": 4.93,
                    #"earnings": 10000
                    }

    return jsonify(courier_data)


@app.route('/orders/assign', methods=['POST'])
def assign():

    courier_id = request.json['courier_id']
    success_data = {'orders':[], 'assign_time': None}

    try:
        courier = Couriers.query.filter_by(courier_id=courier_id).first()
        assert courier is not None
    except AssertionError:
        return Response(f'HTTP 400 Bad Request\n', status=400)

    select_query = f'select DISTINCT o.order_id from orders as o' \
    f' join couriers as c on c.courier_id = {courier_id} and c.max_weight >= o.weight' \
    f' join couriers_regions as c_reg on c_reg.courier_id = {courier_id} and c_reg.region == o.region' \
    f' join couriers_work_hours as c_wh on c_wh.courier_id = {courier_id}' \
    f' and c_wh.start_time <= o.delivery_hour_end and c_wh.end_time >= o.delivery_hour_start' \
    f' where o.assigned_time is null and o.assigned_courier_id is null order by o.order_id'


    result = db.engine.execute(select_query).all()
    assigned_time = datetime.now().isoformat()
    success_data['assign_time'] = assigned_time

    if len(result) == 0:
        return jsonify(result)

    for order_id in result:
        update_query = f"update orders set assigned_courier_id = {courier_id}," \
            f" assigned_time='{assigned_time}'" \
            f" where order_id = {order_id[0]}"

        result = db.engine.execute(update_query)

        success_data['orders'].append({"id": order_id[0]})

        db.session.commit()

    if len(success_data['orders']) != 0:
        return Response(f'HTTP 200 OK\n{json.dumps(success_data)}', status=200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
