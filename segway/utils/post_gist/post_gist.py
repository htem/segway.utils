
import requests
import json

def post_gist(msg, filename, token, description=None):
    """
    Args:
        msg: str
            Message to be posted as a gist.

        filename: str
            Filename field of the gist.

        token: str
            GitHub dev token. See https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
            Make sure that it has the `gist` permission.

        description: str | None
            Description field of the gist.
    """

    assert type(msg) is str
    assert type(filename) is str
    assert type(token) is str

    url = "https://api.github.com/gists"
    headers={'Authorization':'token %s'%token}
    params={'scope':'gist'}
    payload={
        "public":True,
        "files": {
            filename: {"content": msg}
            }
    }
    if description is not None:
        payload['description'] = description

    res = requests.post(url, headers=headers, params=params, data=json.dumps(payload))

    return json.loads(res.text)
