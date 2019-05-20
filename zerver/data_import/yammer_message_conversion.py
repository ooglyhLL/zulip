import re
from typing import Any, Dict, Tuple, List, Optional

import numpy as np
import pandas as pd

# stubs
ZerverFieldsT = Dict[str, Any]
AddedUsersT = Dict[str, int]
AddedChannelsT = Dict[str, Tuple[str, int]]

# Yammer links are just plain URL (stolen from Microsoft page about ASP.NET and changed start/end anchoring)
YM_LINK_REGEX = r"\b(ht|f)tp(s?)\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(:(0-9)*)*(\/?)([a-zA-Z0-9\-\.\?\,\'\/\\\+&amp;%\$#_]*)?"

YM_TAG_REGEX = r"(" + "|".join([r"\[\[user:(?P<user>\d+)\]\]",
                                r"\[Tag:(?P<topicid>\d+):(?P<topic>[a-zA-Z]\w*)\]",
                                r"&\[Tag::n/a\](?P<entity>[0-9]+);",
                                r"\[Tag::(?P<undef>n/a)\]"]) + ")"
"""
Regular expression for matching Yammer tags in messages.

These regexps use uniquely named groups for identifying the match and
selecting appropriate handler code.
"""

# Markdown mapping
def convert_to_zulip_markdown(realm: ZerverFieldsT, users: pd.DataFrame,
                              message: Tuple, added_messages: Dict[str, ZerverFieldsT]
                              ) -> Tuple[str, List[int], bool]:
    mentioned_users_id = []

    # We will need actual markup conversion for late Yammer
    # functionality
    text, mentioned_users_id = message_detaglify(message.body, users)

    has_link = contains_link(text)

    return text, mentioned_users_id, has_link

def message_detaglify(text: str, users: pd.DataFrame) -> Tuple[str, List[np.int64]]:
    """
    Convert Yammer tags to Zulip markdown (and fix some quirks on the way)
    """
    mentioned_users = []
    
    fragments = [] # list of output strings to be joined at the end
    pos = 0
    for m in re.finditer(YM_TAG_REGEX, text):
        if m.start(0) > pos:
            fragments.append(text[pos:m.start(0)])
        pos = m.end(0)
        if m.group('user'):
            uid = np.int64(m.group('user'))
            if uid in users.index:
                username = str(users.loc[uid].name)
                mentioned_users.append(users.loc[uid].zulip_id)
            else:
                username = "Unknown User"
                mentioned_users.append(uid)
            fragments.append("@**")
            fragments.append(username)
            fragments.append("**")
        elif m.group('topic'):
            fragments.append("#")
            fragments.append(m.group('topic'))
        elif m.group('entity'):
            fragments.append(chr(int(m.group('entity'))))
        elif m.group('undef'):
            fragments.append('#')
    fragments.append(text[pos:])
    text = ''.join(fragments)
    return text, mentioned_users

def contains_link(text: str) -> bool:
    """
    Return `True` if `text` contains URL, else false.
    """
    has_link = False
    if re.search(YM_LINK_REGEX, text, re.VERBOSE):
        has_link = True
    return has_link
