# Class to handle OAuth and to abstract server differences
# See https://datatracker.ietf.org/doc/html/rfc6749

import requests
from requests.auth import HTTPBasicAuth
import jwt
import urllib.parse
import base64

from fits_storage.config import get_config
from fits_storage.server.orm.user import User


class OAuth(object):
    def __init__(self):
        self.oauth_server = None
        self.client_id = None
        self.client_secret = None
        self.use_basic_auth = True
        self.verify_signature = True
        self.redirect_url = None
        self.response_id_key = None
        self.user_id_key = None
        self.id_token = None
        self.access_token = None
        self.oauth_id = None
        self.email = None
        self.fullname = None

    def request_access_token(self, code):
        # User came back from OAuth service with a code.
        # Need to POST the code back to the OAuth service to get the credentials
        # result goes in self.id_token.
        # Return None on success string error message on failure

        data = {
            "client_id": self.client_id,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_url
        }
        oauth_token_url = f'https://{self.oauth_server}/token'

        # Some servers require use of HTTP Basic Auth, some just want the
        # client_secret in the post data
        if self.use_basic_auth:
            # And we need to do this with HTTP Basic Auth
            basic = HTTPBasicAuth(self.client_id, self.client_secret)
            r = requests.post(oauth_token_url, data=data, auth=basic)
        else:
            data["client_secret"] = self.client_secret
            r = requests.post(oauth_token_url, data=data)

        print(f'POST Request headers: {r.request.headers}')
        print(f'POST Request body: {r.request.body}')
        print(f'POST Headers: {r.headers}')
        print(f'POST Response text: {r.text}')
        if r.status_code == 200:
            response_data = r.json()
            print(f'Response data: {response_data}')
            self.id_token = response_data.get('id_token')
            self.access_token = response_data.get('access_token')
            return None
        else:
            self.id_token = None
            return f"Bad status code {r.status_code} from OAuth server"

    def decode_id_token(self, verify_signature=None):
        # Return None on success (and set data items), error message on failure
        verify_signature = verify_signature or self.verify_signature
        if verify_signature:
            # Following https://pyjwt.readthedocs.io/en/stable/usage.html
            # #retrieve-rsa-signing-keys-from-a-jwks-endpoint
            config_url = f"https://{self.oauth_server}" \
                         "/.well-known/openid-configuration"
            oidc_config = requests.get(config_url).json()
            signing_algos = oidc_config["id_token_signing_alg_values_supported"]

            # set up a PyJWKClient to get the appropriate signing key
            jwks_client = jwt.PyJWKClient(oidc_config["jwks_uri"])

            # get signing_key from id_token
            signing_key = jwks_client.get_signing_key_from_jwt(self.id_token)

            # now, decode_complete to get payload + header
            data = jwt.api_jwt.decode_complete(
                self.id_token,
                key=signing_key.key,
                algorithms=signing_algos,
                audience=self.client_id,
            )
            payload, header = data["payload"], data["header"]

            # get the pyjwt algorithm object
            alg_obj = jwt.get_algorithm_by_name(header["alg"])

            # compute at_hash, then validate / assert
            digest = alg_obj.compute_hash_digest(
                bytes(self.access_token, 'utf-8'))
            at_hash = base64.urlsafe_b64encode(
                digest[: (len(digest) // 2)]).rstrip(b'=')
            if at_hash == bytes(payload["at_hash"], 'utf-8'):
                # Successful verification
                decoded_id = payload
            else:
                return f"OAuth id token verification failed: " \
                       f"{at_hash} vs {payload['at_hash']}"

        else:
            decoded_id = jwt.decode(self.id_token,
                                    options={"verify_signature": False})
        self.decoded_id = decoded_id
        self.parse_id()

    def parse_id(self):
        pass

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
        setattr(user, self.user_id_key, self.oauth_id)
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
        self.use_basic_auth = True
        self.client_secret = fsc.noirlab_oauth_client_secret
        self.redirect_url = fsc.noirlab_oauth_redirect_url
        self.user_id_key = 'noirlab_id'

    def parse_id(self):
        self.oauth_id = self.decoded_id['sub']
        self.email = self.decoded_id['email']
        self.fullname = f"{self.decoded_id['firstname']} " \
                        f"{self.decoded_id['lastname']}"


class OAuthORCID(OAuth):
    def __init__(self):
        super().__init__()
        fsc = get_config()
        self.oauth_server = fsc.orcid_oauth_server
        self.client_id = fsc.orcid_oauth_client_id
        self.use_basic_auth = False
        self.verify_signature = False
        self.client_secret = fsc.orcid_oauth_client_secret
        self.redirect_url = fsc.orcid_oauth_redirect_url
        self.user_id_key = 'orcid_id'

    def parse_id(self):
        self.oauth_id = self.decoded_id['sub']
        self.fullname = f"{self.decoded_id['given_name']} " \
                        f"{self.decoded_id['family_name']}"
