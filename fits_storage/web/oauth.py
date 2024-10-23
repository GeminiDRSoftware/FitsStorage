# Class to handle OAuth and to abstract server differences
import requests
from requests.auth import HTTPBasicAuth
import jwt
import urllib.parse

from fits_storage.config import get_config
from fits_storage.server.orm.user import User


class OAuth(object):
    def __init__(self):
        self.oauth_server = None
        self.client_id = None
        self.client_secret = None
        self.redirect_url = None
        self.response_id_key = None
        self.user_id_key = None
        self.id_token = None
        self.oauth_id = None
        self.email = None
        self.fullname = None

    def request_access_token(self, code):
        # User came back from OAuth service with a code.
        # Need to POST the code back to the OAuth service to get the credentials
        # result goes in self.id_token.
        # Return None on success string error message on failure
        # And we need to do this with HTTP Basic Auth
        basic = HTTPBasicAuth(self.client_id, self.client_secret)
        data = {
            "client_id": self.client_id,
            # "client_secret": client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_url
        }
        oauth_token_url = f'https://{self.oauth_server}/token'
        # Note, we don't post JSON here, it's an
        # application/x-www-form-urlencoded POST.
        r = requests.post(oauth_token_url, data=data, auth=basic)
        print(f'POST Request headers: {r.request.headers}')
        print(f'POST Request body: {r.request.body}')
        print(f'POST Headers: {r.headers}')
        print(f'POST Response text: {r.text}')
        if r.status_code == 200:
            response_data = r.json()
            self.id_token = response_data.get('id_token')
            return None
        else:
            self.id_token = None
            return f"Bad status code {r.status_code} from OAuth server"

    def decode_id_token(self):
        decoded_id = jwt.decode(self.id_token,
                                options={"verify_signature": False})
        self.oauth_id = decoded_id['sub']
        self.email = decoded_id['email']
        self.fullname = f"{decoded_id['firstname']} {decoded_id['lastname']}"

    def authenticate_url(self):
        oauth_url = f'https://{self.oauth_server}/authorize?client_id=' \
                    f'{self.client_id}' \
                    f'&response_type=code&scope=openid' \
                    f'&redirect_uri={urllib.parse.quote(self.redirect_url)}'
        return oauth_url

    def find_user_by_oauth_id(self, ctx):
        # Find the fits storage user that corresponds to this oauth_id
        # Return None if there is none.
        # Must have called decode_id_token() before this.
        user = ctx.session.query(User)
        user = user.filter(getattr(User, self.user_id_key) == self.oauth_id)
        user = user.one_or_none()
        return user

    def add_oauth_id_to_user(self, ctx, user):
        # Given an existing fits storage user orm instance 'user', add the
        # current oauth_id to that user and commit to the database.
        # Must have called decode_id_token() before this.
        setattr(ctx.user, self.user_id_key, self.oauth_id)
        ctx.session.commit()

    def find_user_by_email(self, ctx):
        # Find the fits storage user that corresponds to this oauth_id's email
        # Return None if there is none.
        # Must have called decode_id_token() before this.
        user = ctx.session.query(User)
        user = user.filter(User.email == self.email)
        user = user.one_or_none()
        return user

    def create_user(self, ctx):
        # Create and commit a new user orm instance from the OAuth info
        # Must have called decode_id_token() before this.
        user = User('')
        setattr(user, self.user_id_key, self.oauth_id)
        user.fullname = self.fullname
        user.email = self.email
        ctx.session.add(user)
        ctx.session.commit()
        return user

class OAuthNOIR(OAuth):
    def __init__(self):
        super().__init__()
        fsc = get_config()
        self.oauth_server = fsc.noirlab_oauth_server
        self.client_id = fsc.noirlab_oauth_client_id
        self.client_secret = fsc.noirlab_oauth_client_secret
        self.redirect_url = fsc.noirlab_oauth_redirect_url
        self.user_id_key = 'noirlab_id'


class OAuthORCID(OAuth):
    def __init__(self):
        super().__init__()
        fsc = get_config()
        self.oauth_server = fsc.orcid_oauth_server
        self.client_id = fsc.orcid_oauth_client_id
        self.client_secret = fsc.orcid_oauth_client_secret
        self.redirect_url = fsc.orcid_oauth_redirect_url
        self.user_id_key = 'orcid_id'
