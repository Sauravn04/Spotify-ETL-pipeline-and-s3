# spotify_api.py

import base64
import requests
import logging


class SpotifyAPI:
    SPOTIFY_API_URL = "https://api.spotify.com/v1/"
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    USER_ENDPOINT = "users"
    PLAYLISTS_ENDPOINT = "playlists"
    TRACKS_ENDPOINT = "tracks"

    def __init__(self, client_id, client_secret, user_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_id = user_id
        self.access_token = self._get_access_token()

    def _get_request(self, url, headers, params=None):
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def _get_auth_headers(self):
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {encoded_credentials}"}

    def _get_access_token(self):
        headers = self._get_auth_headers()
        data = {"grant_type": "client_credentials"}
        try:
            response = requests.post(
                self.TOKEN_URL,
                headers=headers,
                data=data,
                auth=(self.client_id, self.client_secret),
            )
            response.raise_for_status()
            token = response.json().get("access_token")
            if token is None:
                logging.warning(
                    "Access token is None. Response content: %s", response.content
                )
            return token
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting access token: {e}")
            return None

    def _authenticated_request(self, endpoint):
        url = f"{self.SPOTIFY_API_URL}{endpoint}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        return self._get_request(url, headers)

    def get_user_data(self):
        return self._authenticated_request(f"{self.USER_ENDPOINT}/{self.user_id}")

    def get_playlists(self):
        return self._authenticated_request(
            f"{self.USER_ENDPOINT}/{self.user_id}/{self.PLAYLISTS_ENDPOINT}"
        )["items"]

    def get_playlist_tracks(self, playlist_id, limit=100):
        url = f"{self.SPOTIFY_API_URL}{self.PLAYLISTS_ENDPOINT}/{playlist_id}/{self.TRACKS_ENDPOINT}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        all_items = []
        offset = 0

        while True:
            params = {"offset": offset, "limit": limit}
            response = self._get_request(url, headers=headers, params=params)
            items = response.get("items", [])

            if not items:
                break

            all_items.extend(items)
            offset += limit

        return all_items
