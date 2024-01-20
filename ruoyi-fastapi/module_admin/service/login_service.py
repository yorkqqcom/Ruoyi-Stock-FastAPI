from fastapi import Request, Form
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
import random
import uuid
from datetime import timedelta
from module_admin.service.user_service import *
from module_admin.entity.vo.login_vo import *
from module_admin.entity.vo.common_vo import CrudResponseModel
from module_admin.dao.login_dao import *
from config.env import JwtConfig, RedisInitKeyConfig
from utils.common_util import CamelCaseUtil
from utils.pwd_util import *
from utils.response_util import *
from utils.message_util import *
from config.get_db import get_db

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login/loginByAccount")


class CustomOAuth2PasswordRequestForm(OAuth2PasswordRequestForm):
    """
    自定义OAuth2PasswordRequestForm类，增加验证码及会话编号参数
    """
    def __init__(
            self,
            grant_type: str = Form(default=None, regex="password"),
            username: str = Form(),
            password: str = Form(),
            scope: str = Form(default=""),
            client_id: Optional[str] = Form(default=None),
            client_secret: Optional[str] = Form(default=None),
            code: Optional[str] = Form(default=""),
            uuid: Optional[str] = Form(default=""),
            login_info: Optional[Dict[str, str]] = Form(default=None)
    ):
        super().__init__(grant_type=grant_type, username=username, password=password,
                         scope=scope, client_id=client_id, client_secret=client_secret)
        self.code = code
        self.uuid = uuid
        self.login_info = login_info


class LoginService:
    """
    登录模块服务层
    """
    @classmethod
    async def authenticate_user(cls, request: Request, query_db: Session, login_user: UserLogin):
        """
        根据用户名密码校验用户登录
        :param request: Request对象
        :param query_db: orm对象
        :param login_user: 登录用户对象
        :return: 校验结果
        """
        account_lock = await request.app.state.redis.get(
            f"{RedisInitKeyConfig.ACCOUNT_LOCK.get('key')}:{login_user.user_name}")
        if login_user.user_name == account_lock:
            logger.warning("账号已锁定，请稍后再试")
            raise LoginException(data="", message="账号已锁定，请稍后再试")
        # 判断是否开启验证码，开启则验证，否则不验证
        if login_user.captcha_enabled:
            await cls.__check_login_captcha(request, login_user)
        user = login_by_account(query_db, login_user.user_name)
        if not user:
            logger.warning("用户不存在")
            raise LoginException(data="", message="用户不存在")
        if not PwdUtil.verify_password(login_user.password, user[0].password):
            cache_password_error_count = await request.app.state.redis.get(
                f"{RedisInitKeyConfig.PASSWORD_ERROR_COUNT.get('key')}:{login_user.user_name}")
            password_error_counted = 0
            if cache_password_error_count:
                password_error_counted = cache_password_error_count
            password_error_count = int(password_error_counted) + 1
            await request.app.state.redis.set(
                f"{RedisInitKeyConfig.PASSWORD_ERROR_COUNT.get('key')}:{login_user.user_name}", password_error_count,
                ex=timedelta(minutes=10))
            if password_error_count > 5:
                await request.app.state.redis.delete(
                    f"{RedisInitKeyConfig.PASSWORD_ERROR_COUNT.get('key')}:{login_user.user_name}")
                await request.app.state.redis.set(
                    f"{RedisInitKeyConfig.ACCOUNT_LOCK.get('key')}:{login_user.user_name}", login_user.user_name,
                    ex=timedelta(minutes=10))
                logger.warning("10分钟内密码已输错超过5次，账号已锁定，请10分钟后再试")
                raise LoginException(data="", message="10分钟内密码已输错超过5次，账号已锁定，请10分钟后再试")
            logger.warning("密码错误")
            raise LoginException(data="", message="密码错误")
        if user[0].status == '1':
            logger.warning("用户已停用")
            raise LoginException(data="", message="用户已停用")
        await request.app.state.redis.delete(
            f"{RedisInitKeyConfig.PASSWORD_ERROR_COUNT.get('key')}:{login_user.user_name}")
        return user

    @classmethod
    async def __check_login_captcha(cls, request: Request, login_user: UserLogin):
        """
        校验用户登录验证码
        :param request: Request对象
        :param login_user: 登录用户对象
        :return: 校验结果
        """
        captcha_value = await request.app.state.redis.get(
            f"{RedisInitKeyConfig.CAPTCHA_CODES.get('key')}:{login_user.uuid}")
        if not captcha_value:
            logger.warning("验证码已失效")
            raise LoginException(data="", message="验证码已失效")
        if login_user.code != str(captcha_value):
            logger.warning("验证码错误")
            raise LoginException(data="", message="验证码错误")
        return True

    @classmethod
    def create_access_token(cls, data: dict, expires_delta: Union[timedelta, None] = None):
        """
        根据登录信息创建当前用户token
        :param data: 登录信息
        :param expires_delta: token有效期
        :return: token
        """
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=15)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, JwtConfig.SECRET_KEY, algorithm=JwtConfig.ALGORITHM)
        return encoded_jwt

    @classmethod
    async def get_current_user(cls, request: Request = Request, token: str = Depends(oauth2_scheme),
                               query_db: Session = Depends(get_db)):
        """
        根据token获取当前用户信息
        :param request: Request对象
        :param token: 用户token
        :param query_db: orm对象
        :return: 当前用户信息对象
        :raise: 令牌异常AuthException
        """
        # if token[:6] != 'Bearer':
        #     logger.warning("用户token不合法")
        #     raise AuthException(data="", message="用户token不合法")
        try:
            if token.startswith('Bearer'):
                token = token.split(' ')[1]
            payload = jwt.decode(token, JwtConfig.SECRET_KEY, algorithms=[JwtConfig.ALGORITHM])
            user_id: str = payload.get("user_id")
            session_id: str = payload.get("session_id")
            if user_id is None:
                logger.warning("用户token不合法")
                raise AuthException(data="", message="用户token不合法")
            token_data = TokenData(user_id=int(user_id))
        except JWTError:
            logger.warning("用户token已失效，请重新登录")
            raise AuthException(data="", message="用户token已失效，请重新登录")
        query_user = UserDao.get_user_by_id(query_db, user_id=token_data.user_id)
        if query_user.get('user_basic_info') is None:
            logger.warning("用户token不合法")
            raise AuthException(data="", message="用户token不合法")
        redis_token = await request.app.state.redis.get(f"{RedisInitKeyConfig.ACCESS_TOKEN.get('key')}:{session_id}")
        # 此方法可实现同一账号同一时间只能登录一次
        # redis_token = await request.app.state.redis.get(f"{RedisInitKeyConfig.ACCESS_TOKEN.get('key')}:{user.user_basic_info.user_id}")
        if token == redis_token:
            await request.app.state.redis.set(f"{RedisInitKeyConfig.ACCESS_TOKEN.get('key')}:{session_id}", redis_token,
                                              ex=timedelta(minutes=JwtConfig.REDIS_TOKEN_EXPIRE_MINUTES))
            # await request.app.state.redis.set(f"{RedisInitKeyConfig.ACCESS_TOKEN.get('key')}:{user.user_basic_info.user_id}", redis_token,
            #                                   ex=timedelta(minutes=JwtConfig.REDIS_TOKEN_EXPIRE_MINUTES))

            role_id_list = [item.role_id for item in query_user.get('user_role_info')]
            if 1 in role_id_list:
                permissions = ['*:*:*']
            else:
                permissions = [row.perms for row in query_user.get('user_menu_info')]
            post_ids = ','.join([str(row.post_id) for row in query_user.get('user_post_info')])
            role_ids = ','.join([str(row.role_id) for row in query_user.get('user_role_info')])
            roles = [row.role_key for row in query_user.get('user_role_info')]

            current_user = CurrentUserModel(
                permissions=permissions,
                roles=roles,
                user=UserInfoModel(
                    **CamelCaseUtil.transform_result(query_user.get('user_basic_info')),
                    postIds=post_ids,
                    roleIds=role_ids,
                    dept=CamelCaseUtil.transform_result(query_user.get('user_dept_info')),
                    role=CamelCaseUtil.transform_result(query_user.get('user_role_info'))
                )
            )
            return current_user
        else:
            logger.warning("用户token已失效，请重新登录")
            raise AuthException(data="", message="用户token已失效，请重新登录")

    @classmethod
    async def get_current_user_routers(cls, user_id: int, query_db: Session):
        """
        根据用户id获取当前用户路由信息
        :param user_id: 用户id
        :param query_db: orm对象
        :return: 当前用户路由信息对象
        """
        query_user = UserDao.get_user_by_id(query_db, user_id=user_id)
        user_router_menu = [row for row in query_user.get('user_menu_info') if row.menu_type in ['M', 'C']]
        user_router = cls.__generate_user_router_menu(0, user_router_menu)
        return user_router

    @classmethod
    def __generate_user_router_menu(cls, pid: int, permission_list):
        """
        工具方法：根据菜单信息生成路由信息树形嵌套数据
        :param pid: 菜单id
        :param permission_list: 菜单列表信息
        :return: 路由信息树形嵌套数据
        """
        router_list = []
        for permission in permission_list:
            if permission.parent_id == pid:
                children = cls.__generate_user_router_menu(permission.menu_id, permission_list)
                router_list_data = {}
                if permission.menu_type == 'M':
                    router_list_data['name'] = permission.path.capitalize()
                    router_list_data['path'] = f'/{permission.path}'
                    router_list_data['hidden'] = False if permission.visible == '0' else True
                    if permission.is_frame == 1:
                        router_list_data['redirect'] = 'noRedirect'
                    if permission.parent_id == 0:
                        router_list_data['component'] = 'Layout'
                    else:
                        router_list_data['component'] = 'ParentView'
                    if children:
                        router_list_data['alwaysShow'] = True
                        router_list_data['children'] = children
                    router_list_data['meta'] = {
                        'title': permission.menu_name,
                        'icon': permission.icon,
                        'noCache': False if permission.is_cache == '0' else True,
                        'link': permission.path if permission.is_frame == 0 else None
                    }
                elif permission.menu_type == 'C':
                    router_list_data['name'] = permission.path.capitalize()
                    router_list_data['path'] = permission.path
                    router_list_data['hidden'] = False if permission.visible == '0' else True
                    router_list_data['component'] = permission.component
                    router_list_data['meta'] = {
                        'title': permission.menu_name,
                        'icon': permission.icon,
                        'noCache': False if permission.is_cache == '0' else True,
                        'link': permission.path if permission.is_frame == 0 else None
                    }
                router_list.append(router_list_data)

        return router_list


async def get_sms_code_services(request: Request, query_db: Session, user: ResetUserModel):
    """
    获取短信验证码service
    :param request: Request对象
    :param query_db: orm对象
    :param user: 用户对象
    :return: 短信验证码对象
    """
    redis_sms_result = await request.app.state.redis.get(f"{RedisInitKeyConfig.SMS_CODE.get('key')}:{user.session_id}")
    if redis_sms_result:
        return SmsCode(**dict(is_success=False, sms_code='', session_id='', message='短信验证码仍在有效期内'))
    is_user = UserDao.get_user_by_name(query_db, user.user_name)
    if is_user:
        sms_code = str(random.randint(100000, 999999))
        session_id = str(uuid.uuid4())
        await request.app.state.redis.set(f"{RedisInitKeyConfig.SMS_CODE.get('key')}:{session_id}", sms_code, ex=timedelta(minutes=2))
        # 此处模拟调用短信服务
        message_service(sms_code)

        return SmsCode(**dict(is_success=True, sms_code=sms_code, session_id=session_id, message='获取成功'))

    return SmsCode(**dict(is_success=False, sms_code='', session_id='', message='用户不存在'))


async def forget_user_services(request: Request, query_db: Session, forget_user: ResetUserModel):
    """
    用户忘记密码services
    :param request: Request对象
    :param query_db: orm对象
    :param forget_user: 重置用户对象
    :return: 重置结果
    """
    redis_sms_result = await request.app.state.redis.get(f"{RedisInitKeyConfig.SMS_CODE.get('key')}:{forget_user.session_id}")
    if forget_user.sms_code == redis_sms_result:
        forget_user.password = PwdUtil.get_password_hash(forget_user.password)
        forget_user.user_id = UserDao.get_user_by_name(query_db, forget_user.user_name).user_id
        edit_result = UserService.reset_user_services(query_db, forget_user)
        result = edit_result.dict()
    elif not redis_sms_result:
        result = dict(is_success=False, message='短信验证码已过期')
    else:
        await request.app.state.redis.delete(f"{RedisInitKeyConfig.SMS_CODE.get('key')}:{forget_user.session_id}")
        result = dict(is_success=False, message='短信验证码不正确')

    return CrudResponseModel(**result)


async def logout_services(request: Request, session_id: str):
    """
    退出登录services
    :param request: Request对象
    :param session_id: 会话编号
    :return: 退出登录结果
    """
    await request.app.state.redis.delete(f"{RedisInitKeyConfig.ACCESS_TOKEN.get('key')}:{session_id}")
    # await request.app.state.redis.delete(f'{current_user.user.user_id}_access_token')
    # await request.app.state.redis.delete(f'{current_user.user.user_id}_session_id')

    return True
