import json
from psycopg2 import connect as psycopg2_connect, sql
from psycopg2 import extras
from psycopg2.errors import UniqueViolation
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
            user_name = decoded_jwt["user_name"]
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
        "result": None,
        "message": None
    }


    token = event["headers"]["Authorization"]    
    event = event.get('body', event)
    evet_bridge_input = {
        "notification_input":{},
        "analysis_input": {}
    }

    #-------------- CONNECTING TO DATABASE ---------------
    try:

        conn = psycopg2_connect(host=os.environ['POSTGRES_ENDPOINT'],
                                port=os.environ['POSTGRES_PORT'],
                                database=os.environ['POSTGRES_DBNAME'],
                                user=os.environ['POSTGRES_DBUSER'],
                                password=os.environ['POSTGRES_DBPASS'])

        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
    except Exception as e:
        response['error'] = f"Error in Connecting to database: {str(e)}"
        return response


    #------------------- VALIDATE TOKEN AND GET USER_ID ----------
    try:
        authentication_res = authenticate(token=token)
        user_id = int(authentication_res['user_id'])
        user_name = authentication_res['user_name']
    except Exception as e:
        response['error'] = f"Error on authentication and getting user_id : {str(e)}"

    #------------- GET AND VALITADE INPUTS ------------------    
    try:
        brand_id = event.get('brand_id')
        if not isinstance(dis_name, int):
                    raise Exception('Brand id should be integer!!!')
    except Exception as e:
        response['error'] = f"Invalid input :{str(e)}"
        return response

    #--------- VALIDATE BRAND -----------------
    try:
        sql_statement=sql.SQL("""SELECT name FROM brands WHERE id=%(b_id)s""")
        sql_kwargs = {"b_id": brand_id}
        cursor.execute(sql_statement,sql_kwargs)
        result = cursor.fetchone()
        if result.get('name', None):
            raise Exception("Brand not found!!!")
        brand_name = result['name']
    except Exception as e:
        response['error'] = str(e)
        return response

    #------ GET DISCOUNTS FOR THE BRAND --------------
    try:
        sql_statement = sql.SQL(""" SELECT name, code, amount, amount_type, expiration_date, minimum_basket
                                    FROM discounts
                                    WHERE brand_id=%(b_id)s""")
        sql_kwargs = {"b_id": brand_id}
        cursor.execute(sql_statement,sql_kwargs)
        discounts = [dict(record) for record in cursor]
    except Exception as e:
        response['error'] = str(e)
        return response

    if len(discounts)>0:
        response['result'] = discounts

        # -------------- SHARE USER INFO WITH BRAND -------------------
        try:
            sql_statement = sql.SQL(""" INSERT INTO shared_info (brand_id, user_id, creation_date)
                                        VALUES (%(b_id)s, %(u_id)s, %(c_date)s)""")
            sql_kwargs = {"b_id": brand_id,
                          "u_id": user_id}
            cursor.execute(sql_statement,sql_kwargs)
            evet_bridge_input["notification_input"] = {
                "receiver_id": brand_id,
                "receiver_type": "brand",
                "notification_type": "shared_info",
                "message": f"{user_name} has shared his/her contact info with your brand"
            }
            evet_bridge_input['analysis_input'] = {
                "receiver_id": brand_id,
                "receiver_type": "brand",
                "sender_id": user_id,
                "sender_name": user_name
            }
        except UniqueViolation as db_exc:
            if "shared_info_user_id_brand_id_258cd3f0_uniq" in str(db_exc):
                response["message"] = f"You already shared your info with {brand_name}!!!"
            else:
                response["error"] = str(db_exc)
                return response
        except Exception as e:
            response['error'] = str(e)
            return response
    else:
        # ----------- IF THERE IS NO DISCOUNT CODE INFO WILL NOT BE SHARED WITH BRAND--------
        response['message'] = f"There are no Discount code available for {brand_name} brand!!!"
        response['is_success'] = True
        return response


    #------------ PUT EVENT --------------------------
    event_bridge = boto3.client('events')
    put_event_result = event_bridge.put_events(
                                Entries=[
                                    {
                                        "Source": "discounts",
                                        "DetailType": "get_discount_code", 
                                        "Detail": json.dumps(event_bridge_input, default=decimal_default),
                                        "EventBusName": "billogram-event-bus"
                                    },
                                ]
                            )   
    if put_event_result["ResponseMetadata"]["HTTPStatusCode"] != 200:
        response['error'] = f"Error in put event : {str(e)}."
        return response

    #--------- COMMIT CHANGES AND CLOSE DATABASE CONNECTION
    conn.commit()
    conn.close()
    
    response['is_success'] = True
    return response