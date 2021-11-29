import json
from psycopg2 import connect as psycopg2_connect, sql
from psycopg2 import extras
import os
import boto3
import datetime
import jwt




def decode_rs256_token(token: str) -> dict:
    public_key_list_url = os.environ['COGNITO_PUBLIC_KEYS_URL']
    jwks_client = jwt.PyJWKClient(public_key_list_url)
    signing_key = jwks_client.get_signing_key_from_jwt(token)

    payload = jwt.decode(token, signing_key.key, algorithms=["RS256"], options={"verify_aud": False})
    return payload


def decode_token(token: str) -> dict:
    payload = jwt.decode(token, options={
        "verify_signature": False,
        "verify_aud": False,
        "verify_iat": False,
        "verify_exp": False,
        "verify_iss": False,
        "verify_nbf": False
    }, verify=False)
    return payload


def authenticate(token, verify=True):
    try:
        if verify:
            decoded_jwt = decode_rs256_token(token)
        else:
            decoded_jwt = decode_token(token)

        try:
            user_id = decoded_jwt["user_id"]
        except LookupError:
            raise Exception("Incomplete token.")     
    except Exception as e:
        raise e        
    

    return user_id





def lambda_handler(event, context):
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except (TypeError, json.JSONDecodeError):
            pass
        assert isinstance(event, dict)

    response = {
        "error": None,
        "is_success": False,
        "result": None
    }


    token = event["headers"]["Authorization"]    
    event = event.get('body', event)

    #-------------- CONNECTING TO DATABASE ---------------
    try:

        conn = psycopg2_connect(host=os.environ['POSTGRES_ENDPOINT'],
                                port=os.environ['POSTGRES_PORT'],
                                database=os.environ['POSTGRES_DBNAME'],
                                user=os.environ['POSTGRES_DBUSER'],
                                password=os.environ['POSTGRES_DBPASS'])

        cursor = conn.cursor(cursor_factory=extras.DictCursor)
    except Exception as e:
        response['error'] = f"Error in Connecting to database: {str(e)}"
        return response


    #------------------- VALIDATE TOKEN AND GET USER_ID ----------
    try:
        brand_id = int(authenticate(token=token)['user_id'])
    except Exception as e:
        response['error'] = f"Error on authentication and getting user_id : {str(e)}"

    for item in event:
        #--------- GET AND VALIDATE INPUTS -------------
        try:
            dis_name = item.get('discount_name')
            if not isinstance(dis_name, str):
                raise Exception('invalid name')
            dis_value = item.get('discount_value')
            if not isinstance(dis_value, float):
                raise Exception('invalid value')
            dis_type = item.get('discount_type')
            if not isinstance(dis_type, str) or dis_type not in ['percent', 'literal']:
                raise Exception('invalid type')
            min_basket = item.get('minimum_basket')
            if not isinstance(min_basket, float):
                raise Exception('invalid value')
            exp = item.get('valid_days')
            if not isinstance(exp, int):
                raise Exception('invalid valid days')
        except Exception as e:
            response['error'] = f"Invalid input : {str(e)}"
            return response


        exp_days = datetime. timedelta(days=exp)

        #---------- GENERATE CODE --------------
        dis_code = "{}-{}".format(dis_name[0:2].upper(), str(uuid.uuid4()))

        #---------- INSERT INTO DATABASE -------
        sql_statement = sql.SQL("""INSERT INTO discounts 
                                   (brand_id, code, name, amount, amount_type, minimum_basket, expiration_date, creation_date)
                                   VALUES %(b_id)s, %(code)s, %(name)s, %(amount)s, %(amount_type)s,%(min_basket)s, %(exp)s, %(c_date)s""")
        sql_kwargs = {
            "b_id": brand_id,
            "code": dis_code,
            "name": dis_name,
            "amount": dis_value,
            "amount_type": dis_type,
            "min_basket": min_basket,
            "exp": datetime.now(tz=pytz.UTC) + exp_days
            "c_date": datetime.now(tz=pytz.UTC)
        }

        try:
            cursor.execute(sql_statement, sql_kwargs)
        except Exception as e:
            response['error'] = f"Error in Insert into database: {str(e)}"
            return response

    response['is_success'] = True
    response['result'] = "Discount codes has been added successfully!!!"
    return response