import boto3
import pandas as pd
from tqdm import tqdm


def get_all_users(user_pool_id):
    """ Cognitoユーザプールからユーザリストを取得
    Args:
        user_pool_id (string): CognitoユーザプールID
    Returns:
        users (list): ユーザプール内の全てのユーザリスト
    """
    cognito_client = boto3.client('cognito-idp')
    users = []
    pagination_token = None

    while True:
        if pagination_token:
            response = cognito_client.list_users(
                UserPoolId=user_pool_id,
                PaginationToken=pagination_token
            )
        else:
            response = cognito_client.list_users(
                UserPoolId=user_pool_id
            )

        users.extend(response['Users'])

        if 'PaginationToken' in response:
            pagination_token = response['PaginationToken']
        else:
            break

    return users


def users_to_dataframe(users):
    """ Cognitoユーザプールから取得したユーザリストをデータフレームに変換
    Args:
        users (list): ユーザプール内の全てのユーザリスト
    Returns:
        (pd.DataFrame): Cognitoのユーザリスト
    """
    user_data = []
    for user in users:
        user_dict = {
            'Username': user['Username'],
            'UserStatus': user['UserStatus'],
            'UserCreateDate': user['UserCreateDate'].strftime('%Y-%m-%d %H:%M:%S'),
            'UserLastModifiedDate': user['UserLastModifiedDate'].strftime('%Y-%m-%d %H:%M:%S'),
        }

        # Add user attributes
        for attr in user['Attributes']:
            user_dict[attr['Name']] = attr['Value']

        user_data.append(user_dict)

    return pd.DataFrame(user_data)


def check_string_in_column(df, column_name, string_to_check):
    """
    Args:
        df(Dataframe): チェック対象のデータフレーム
        column_name(str): チェック対象の列名
        string_to_check(str): 検索する文字列
    Return:
        (list): データフレームの各行について、True|False のリスト
    """
    return df[column_name].str.contains(string_to_check, na=False)


def create_cognito_users(USER_POOL_ID, df):
    """ Cognito ユーザプールに新規ユーザをまとめて作成する
    Args:
        USER_POOL_ID (str): CognitoユーザプールID
        df (pd.DataFrame): 登録したいユーザと初期パスワードをまとめたデータフレーム
    """
    cognito_client = boto3.client('cognito-idp')

    # Cognito ユーザプールから最新のユーザリストを取得
    user_list = get_all_users(USER_POOL_ID)
    # print(f"Cognito ユーザプールに登録されている人数: {len(user_list)}")

    # ユーザリストをデータフレームに変換
    df_users = users_to_dataframe(user_list)

    # 一時的な仮パスワード
    PASS_TEMP = "Karipass#9020"

    # Admin権限でユーザを新規作成する
    for index, row in tqdm(df.iterrows(), total=len(df)):
        EMAIL = row["メールアドレス"]
        PASS = row["初期パスワード"]

        # Cognitoユーザプール内に重複が無いかをチェック
        if check_string_in_column(df_users, 'email', EMAIL).sum():
            print(f'Creatig user >>> {EMAIL} >>> pass')
            continue
        print(f'Creatig user >>> {EMAIL}')

        # 共通の仮パスワードを使って、とりあえずユーザ作成
        # ここで一時パスワードを設定すると時限付となってしまう
        # メール認証は `済` にしてしまう
        cognito_client.admin_create_user(
            UserPoolId=USER_POOL_ID,
            Username=EMAIL,
            TemporaryPassword=PASS_TEMP,
            UserAttributes=[
                {
                    'Name': 'email_verified',
                    'Value': 'true'
                },
                {
                    'Name': 'email',
                    'Value': EMAIL
                }
            ],
            MessageAction='SUPPRESS'
        )

        # 初期パスワードを確認済みとして設定
        cognito_client.admin_set_user_password(
            UserPoolId=USER_POOL_ID,
            Username=EMAIL,
            Password=PASS,
            Permanent=True
        )

    print("New user created in the Cognito user pool.")


def main():
    df = pd.read_excel('GenU_一括登録リスト（テスト）.xlsx')
    df = df.dropna(subset='メールアドレス')
    df = df.filter(['メールアドレス', '初期パスワード'])    # 余計な情報があった場合は、フィルタリングする

    # Cognito ユーザプールID
    USER_POOL_ID = "XXXXXXXXXXXXXXXXXXXXXXX"

    # ユーザ名と初期パスワードをまとめたデータフレーム
    df = pd.read_csv("users.csv")

    create_cognito_users(USER_POOL_ID, df)

    print("Done.")


if __name__ == "__main__":
    main()
