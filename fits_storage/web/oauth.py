# Class to handle OAuth and to abstract server differences
# See https://datatracker.ietf.org/doc/html/rfc6749

import requests
from requests.auth import HTTPBasicAuth
import jwt
import urllib.parse
import base64

from fits_storage.config import get_config
from fits_storage.server.orm.user import User


class OAuthError(Exception):
    pass


class OAuth(object):
    def __init__(self):
        self.client_id = None
        self.client_secret = None
        self.use_basic_auth = True
        self.config_endpoint = None
        self.verify_signature = True
        self.redirect_url = None
        self.response_id_key = None
        self.user_id_key = None
        self.id_token = None
        self.access_token = None
        self.decoded_id_token = None
        self.oauth_id = None
        self.email = None
        self.fullname = None
        self._openid_config = None

    @property
    def openid_config(self):
        if self._openid_config:
            return self._openid_config
        if self.config_endpoint is None:
            raise OAuthError('No config_endpoint '
                             '(.well-known/openid-configuration) in config')
        response = requests.get(self.config_endpoint)
        if response.status_code != 200:
            raise OAuthError('Bad status code %d getting openid-configuration' %
                             response.status_code)
        if not response.headers.get('content-type')\
                .startswith('application/json'):
            raise OAuthError('Got non-json content type %s getting '
                             'openid-configuration' %
                             response.headers['content-type'])

        self._openid_config = response.json()

        # Sanity check results here
        if self.openid_config.get('authorization_endpoint') is None:
            raise OAuthError('No authorization_endpoint	in '
                             'openid-configuration')
        if self.openid_config.get('token_endpoint') is None:
            raise OAuthError('No token_endpoint	in openid-configuration')

        if self.verify_signature is True:
            if self.openid_config.get('id_token_signing_alg_values_supported') \
                    is None:
                # We could simply set self.verify_signature to False and not
                # raise an exception here, but it's better that's a deliberate
                # decision and set in the init for this Oauth server handler.
                raise OAuthError('No id_token_signing_alg_values_supported in '
                                 'openid-configuration')
            if self.openid_config.get('jwks_uri') is None:
                raise OAuthError('No jwks_url in openid-configuration')

        # Sanity check other parts of the configuration here, this code gets
        # called before any methods that use these, it's basically a lazy init.
        if self.client_id is None:
            raise OAuthError('client_id is not defined')
        if self.redirect_url is None:
            raise OAuthError('redirect_url is not defined')

        return self._openid_config

    def authorization_url(self):
        # This is the URL we send the user to for them to authenticate. This
        # URL contains the redirect_url which the authorization server with
        # send them back to with an authorization code.
        return f"{self.openid_config['authorization_endpoint']}" \
               f"?client_id={self.client_id}&response_type=code&scope=openid" \
               f"&redirect_uri={urllib.parse.quote(self.redirect_url)}"

    def request_tokens(self, code):
        # User came back from authorization URL with a code.
        # We Need to POST the code back to the OAuth service to check that it is
        # valid, and get an access_token and id_token if so.
        # results are stored in self.access_token and self.id_token.

        data = {
            "client_id": self.client_id,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_url
        }
        oauth_token_url = self.openid_config['token_endpoint']

        # Some servers require use of HTTP Basic Auth, some just want the
        # client_secret in the post data
        if self.use_basic_auth:
            basic = HTTPBasicAuth(self.client_id, self.client_secret)
            r = requests.post(oauth_token_url, data=data, auth=basic)
        else:
            data["client_secret"] = self.client_secret
            r = requests.post(oauth_token_url, data=data)

        if r.status_code != 200:
            raise OAuthError(f"Bad status code {r.status_code} from OAuth "
                             f"server token endpoint")
        response_data = r.json()
        self.id_token = response_data.get('id_token')
        self.access_token = response_data.get('access_token')

        if self.id_token is None:
            raise OAuthError('Did not get id_token from token endpoint')
        if self.access_token is None:
            raise OAuthError('Did not get access_token from token endpoint')

    def decode_id_token(self, verify_signature=None):
        # Verify signature default value in the instance data
        verify_signature = verify_signature or self.verify_signature
        if verify_signature:
            # Following https://pyjwt.readthedocs.io/en/stable/usage.html
            # retrieve rsa signing keys from a jwks endpoint
            signing_algos = \
                self.openid_config["id_token_signing_alg_values_supported"]

            # set up a PyJWKClient to get the appropriate signing key
            jwks_client = jwt.PyJWKClient(self.openid_config["jwks_uri"])

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
                self.decoded_id_token = payload
            else:
                raise OAuthError(f"OAuth id token verification failed: "
                                 f"{at_hash} vs {payload['at_hash']}")

        else:
            self.decoded_id_token = \
                jwt.decode(self.id_token, options={"verify_signature": False})
        self.parse_id_token()

    def parse_id_token(self):
        # Subclasses must over-ride this method, they should know what is
        # expected to be encapsulated in the id_token from their auth service
        raise OAuthError('Subclass did not override parse_id_token()')

    def find_user_by_oauth_id(self, ctx):
        # Find the fits storage user that corresponds to this oauth_id
        # Return None if there is none.

        # Must have called parse_id_token() (via decode_id_token()) before this.
        if self.oauth_id is None:
            raise OAuthError("oauth_id not set in find_user_by_oauth_id")

        return ctx.session.query(User)\
            .filter(getattr(User, self.user_id_key) == self.oauth_id)\
            .one_or_none()

    def add_oauth_id_to_user(self, ctx, user):
        # Given an existing fits storage user orm instance 'user', add the
        # current oauth_id to that user and commit to the database.

        # Must have called parse_id_token() (via decode_id_token()) before this.
        if self.oauth_id is None:
            raise OAuthError("oauth_id not set in add_oauth_id_to_user")

        setattr(user, self.user_id_key, self.oauth_id)
        ctx.session.commit()

    def find_user_by_email(self, ctx):
        # Find the fits storage user that corresponds to this oauth_id's email
        # Return None if there is none.

        # Must have called parse_id_token() (via decode_id_token()) before this.
        if self.oauth_id is None:
            raise OAuthError("oauth_id not set in find_user_by_email")

        return ctx.session.query(User).filter(User.email == self.email)\
            .one_or_none()

    def create_user(self, ctx):
        # Create and commit a new user orm instance from the OAuth info

        # Must have called parse_id_token() (via decode_id_token()) before this.
        if self.oauth_id is None:
            raise OAuthError("oauth_id not set in create_user")

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
        self.config_endpoint = fsc.noirlab_oauth_config
        self.client_id = fsc.noirlab_oauth_client_id
        self.client_secret = fsc.noirlab_oauth_client_secret
        self.redirect_url = fsc.noirlab_oauth_redirect_url
        self.user_id_key = 'noirlab_id'

    def parse_id_token(self):
        self.oauth_id = self.decoded_id_token['sub']
        self.email = self.decoded_id_token['email']
        self.fullname = f"{self.decoded_id_token['firstname']} " \
                        f"{self.decoded_id_token['lastname']}"


class OAuthORCID(OAuth):
    def __init__(self):
        super().__init__()
        fsc = get_config()
        self.config_endpoint = fsc.orcid_oauth_config
        self.client_id = fsc.orcid_oauth_client_id
        self.use_basic_auth = False
        self.client_secret = fsc.orcid_oauth_client_secret
        self.redirect_url = fsc.orcid_oauth_redirect_url
        self.user_id_key = 'orcid_id'

    def parse_id_token(self):
        self.oauth_id = self.decoded_id_token['sub']
        self.fullname = f"{self.decoded_id_token['given_name']} " \
                        f"{self.decoded_id_token['family_name']}"

        # Try to read more stuff from ORCID
        url = f"https://api.sandbox.orcid.org/v3.0/{self.oauth_id}/read-public"
        headers = {'Accept': 'application/orcid+json',
                   'Authorization': f'Bearer {self.access_token}'}
        r = requests.get(url, headers=headers)
        print(f'status_code: {r.status_code}')
        print(f'test: {r.text}')
