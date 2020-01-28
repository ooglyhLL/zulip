import os
import re
import shutil
import subprocess
import logging
import random
import requests
import zipfile

from collections import defaultdict
from pathlib import PurePath
import mimetypes

import numpy as np
import pandas as pd
import dateutil.parser
from dateutil.relativedelta import relativedelta
import datetime
from datetime import timezone
from collections import namedtuple

from django.conf import settings
from django.utils.timezone import now as timezone_now
from django.forms.models import model_to_dict
from typing import Any, Dict, List, Optional, Tuple, Set, Iterator
from zerver.models import Reaction, RealmEmoji, UserProfile, Recipient, \
    CustomProfileField, CustomProfileFieldValue
from zerver.data_import.yammer_message_conversion import convert_to_zulip_markdown
from zerver.data_import.import_util import ZerverFieldsT, build_zerver_realm, \
    build_avatar, build_subscription, build_recipient, build_usermessages, \
    build_defaultstream, build_attachment, process_avatars, process_uploads, \
    process_emojis, build_realm, build_stream, build_message, \
    create_converted_data_files, make_subscriber_map
from zerver.data_import.sequencer import NEXT_ID
from zerver.lib.upload import random_name, sanitize_name
from zerver.lib.export import MESSAGE_BATCH_CHUNK_SIZE
from zerver.lib.emoji import NAME_TO_CODEPOINT_PATH
from zerver.lib.url_encoding import hash_util_encode, encode_stream

# stubs
AddedUsersT = Dict[str, int]
AddedGroupsT = Dict[str, Tuple[str, int]]
AddedRecipientsT = Dict[str, int]

def rm_tree(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)

def yammer_network_to_realm(domain_name: str, realm_id: int, realm_subdomain: str,
                            yammer_data: Dict[str, pd.DataFrame]) -> Tuple[ZerverFieldsT, AddedUsersT,
                                                                          AddedRecipientsT,
                                                                          List[ZerverFieldsT],
                                                                          ZerverFieldsT]:
    """
    Returns:
    1. realm, Converted Realm data
    2. added_users, which is a dictionary to map from Yammer user id to zulip user id
    3. added_recipient, which is a dictionary to map from channel name to zulip recipient_id
    4. added_groups, which is a dictionary to map from channel name to channel id, zulip stream_id
    5. avatars, which is list to map avatars to zulip avatar records.json
    """
    NOW = float(timezone_now().timestamp())

    zerver_realm = build_zerver_realm(realm_id, realm_subdomain, NOW, 'Yammer')  # type: List[ZerverFieldsT]
    realm = build_realm(zerver_realm, realm_id, domain_name)

    zerver_userprofile, avatars, added_users, zerver_customprofilefield, \
        zerver_customprofilefield_value = users_to_zerver_userprofile(yammer_data, realm_id,
                                                                      int(NOW), domain_name)

    groups_to_zerver_stream_fields = groups_to_zerver_stream(yammer_data,
                                                             realm_id,
                                                             added_users,
                                                             zerver_userprofile)
    emoji_url_map = {}
    realm['zerver_realmemoji'] = []

    # See https://zulipchat.com/help/set-default-streams-for-new-users
    # for documentation on zerver_defaultstream
    realm['zerver_userprofile'] = zerver_userprofile

    # Custom profile fields
    realm['zerver_customprofilefield'] = zerver_customprofilefield
    realm['zerver_customprofilefieldvalue'] = zerver_customprofilefield_value

    realm['zerver_defaultstream'] = groups_to_zerver_stream_fields[0]
    realm['zerver_stream'] = groups_to_zerver_stream_fields[1]
    realm['zerver_subscription'] = groups_to_zerver_stream_fields[2]
    realm['zerver_recipient'] = groups_to_zerver_stream_fields[3]
    added_recipient = groups_to_zerver_stream_fields[4]

    return realm, added_users, added_recipient, avatars, emoji_url_map

def users_to_zerver_userprofile(yammer_data: Dict[str, pd.DataFrame], realm_id: int,
                                timestamp: Any, domain_name: str) -> Tuple[List[ZerverFieldsT],
                                                                           List[ZerverFieldsT],
                                                                           AddedUsersT,
                                                                           List[ZerverFieldsT],
                                                                           List[ZerverFieldsT]]:
    """
    Returns:
    1. zerver_userprofile, which is a list of user profile
    # 2. avatar_list, which is list to map avatars to zulip avatard records.json
    3. added_users, which is a dictionary to map from Yammer user id to zulip
       user id. This is kept for compatibility as the id values are the same
    4. zerver_customprofilefield, which is a list of all custom profile fields
    5. zerver_customprofilefield_values, which is a list of user profile fields
    """
    logging.info('######### IMPORTING USERS STARTED #########\n')
    zerver_userprofile = []
    zerver_customprofilefield = []  # type: List[ZerverFieldsT]
    zerver_customprofilefield_values = []  # type: List[ZerverFieldsT]
    avatar_list = []  # type: List[ZerverFieldsT]
    added_users = {}

    users = yammer_data['Users']
    users = users.assign(is_realm_admin=users.id.isin(yammer_data['Admins'].id))

    msgs = yammer_data['Messages']
    unknown_user_ids = msgs[~msgs.sender_id.isin(users.id)].sender_id.drop_duplicates()
    unknown_user_count = 0

    # To map user id with the custom profile fields of the corresponding user
    slack_user_custom_field_map = {}  # type: ZerverFieldsT
    # To store custom fields corresponding to their ids
    custom_field_map = {}  # type: ZerverFieldsT

    def get_user_email(user: Tuple, domain: str):
        if user and pd.notna(user.email):
            return user.email
        else:
            # Make up unique email address which will hopefully be
            # delivered to the postmaster if actually used.
            import uuid
            return str(uuid.uuid4()) + "@" + domain

    for user in users.itertuples():
        userprofile = UserProfile(
            full_name=user.name,
            short_name="",
            is_active=((pd.notna(user.state) and user.state == "active")
                       or pd.isna(user.deleted_by_id)
                       and pd.isna(user.suspended_by_id)),
            id=int(user.zulip_id),
            email=get_user_email(user, domain_name),
            delivery_email=get_user_email(user, domain_name),
            avatar_source='U',
            is_bot=False,
            pointer=-1,
            is_realm_admin=user.is_realm_admin,
            bot_type=None,
            date_joined=user.joined_at,
            timezone="UTC",
            last_login=timestamp)
        userprofile_dict = model_to_dict(userprofile)
        # Set realm id separately as the corresponding realm is not yet a Realm model instance
        userprofile_dict['realm'] = realm_id
        zerver_userprofile.append(userprofile_dict)
        added_users[user.id] = user.zulip_id

        logging.info(u"{} -> {}".format(userprofile_dict['full_name'], userprofile_dict['email']))

    for msg, user_id in unknown_user_ids.iteritems():
        username = "Unknown User " + str(unknown_user_count)

        userprofile = UserProfile(
            full_name=username,
            short_name="",
            is_active=False,
            id=int(user_id),
            email=get_user_email(None, domain_name),
            delivery_email=get_user_email(None, domain_name),
            avatar_source='U',
            is_bot=False,
            pointer=-1,
            is_realm_admin=False,
            bot_type=None,
            date_joined=yammer_data['Networks'].iloc[0].created_at,
            timezone="UTC",
            last_login=timestamp)
        userprofile_dict = model_to_dict(userprofile)
        # Set realm id separately as the corresponding realm is not yet a Realm model instance
        userprofile_dict['realm'] = realm_id
        zerver_userprofile.append(userprofile_dict)
        added_users[user_id] = user_id
        unknown_user_count += 1

        logging.info(u"{}".format(userprofile_dict['full_name']))

    logging.info('######### IMPORTING USERS FINISHED #########\n')
    return zerver_userprofile, avatar_list, added_users, zerver_customprofilefield, \
        zerver_customprofilefield_values

def groups_to_zerver_stream(yammer_data: Dict[str, pd.DataFrame], realm_id: int, added_users: AddedUsersT,
                            zerver_userprofile: List[ZerverFieldsT]) -> Tuple[List[ZerverFieldsT],
                                                                              List[ZerverFieldsT],
                                                                              List[ZerverFieldsT],
                                                                              AddedRecipientsT]:
    """
    Returns:
    1. zerver_defaultstream, which is a list of the default streams
    2. zerver_stream, while is a list of all streams
    3. zerver_subscription, which is a list of the subscriptions                            
    4. zerver_recipient, which is a list of the recipients                                  
    5. added_recipient, which is a dictionary to map from channel name to zulip recipient_id
    """
    logging.info('######### IMPORTING GROUPS STARTED #########\n')

    added_recipient = {}

    zerver_stream = []
    zerver_subscription = []  # type: List[ZerverFieldsT]
    zerver_recipient = []
    zerver_defaultstream = []

    subscription_id_count = recipient_id_count = 0
    defaultstream_id = 1

    logging.info('Creating stream from implicit group "All Company"\n')
    all_company_desc = "This is the default stream for all Yammer messages."
    realm_created_at = float(yammer_data['Networks'][yammer_data['Networks'].zulip_id == realm_id].created_at)
    stream = build_stream(realm_created_at,
                          realm_id, "Yammer/All Company",
                          all_company_desc, defaultstream_id, False, False)
    zerver_stream.append(stream)
    defaultstream = build_defaultstream(realm_id, defaultstream_id, defaultstream_id)
    zerver_defaultstream.append(defaultstream)

    recipient_id = recipient_id_count
    recipient = build_recipient(defaultstream_id, recipient_id, Recipient.STREAM)
    zerver_recipient.append(recipient)
    added_recipient[stream['name']] = recipient_id

    recipient_id_count += 1

    for group in yammer_data['Groups'].itertuples():
        stream_id = group.zulip_id
        recipient_id = recipient_id_count

        description = group.description if pd.notna(group.description) else ""

        # construct the stream object and append it to zerver_stream
        logging.info('Creating stream from group "%s"\n' % group.name)
        stream = build_stream(group.created_at, realm_id,
                              ("Yammer/" + group.name),
                              description, stream_id, False, group.private)
        zerver_stream.append(stream)

        recipient = build_recipient(stream_id, recipient_id, Recipient.STREAM)
        zerver_recipient.append(recipient)
        added_recipient[stream['name']] = recipient_id
        # TODO add recipients for private message and huddles

# Group subscriptions in Yammer can only be obtained via REST API
#        # construct the subscription object and append it to zerver_subscription
#        subscription_id_count = get_subscription(channel['members'], zerver_subscription,
#                                                 recipient_id, added_users,
#                                                 subscription_id_count)

        recipient_id_count += 1
        logging.info(u"{} -> created".format(stream['name']))

    for user in yammer_data['Users'].itertuples():
        # subscribe all users to Yammer/All Company
        sub = build_subscription(added_recipient["Yammer/All Company"], user.zulip_id, subscription_id_count)
        zerver_subscription.append(sub)
        subscription_id_count += 1

        # this maps the recipients and subscriptions
        # related to private messages
        recipient = build_recipient(user.zulip_id, recipient_id_count, Recipient.PERSONAL)
        sub = build_subscription(recipient_id_count, user.zulip_id, subscription_id_count)

        zerver_recipient.append(recipient)
        zerver_subscription.append(sub)

        subscription_id_count += 1
        recipient_id_count += 1

    logging.info('######### IMPORTING STREAMS FINISHED #########\n')
    return zerver_defaultstream, zerver_stream, zerver_subscription, \
        zerver_recipient, added_recipient

def process_long_term_idle_users(yammer_data: Dict[str, pd.DataFrame],
                                 zerver_userprofile: List[ZerverFieldsT]) -> Set[int]:
    """Algorithmically, we treat users who have sent at least 10 messages
    or have sent a message within the last 60 days as active.
    Everyone else is treated as long-term idle, which means they will
    have a slighly slower first page load when coming back to
    Zulip.
    """
    NOW = datetime.datetime.now(timezone.utc)
    td = relativedelta(days=60)

    all_messages = yammer_data['Messages']
    recent_messages = all_messages[all_messages.created_at > (NOW - td).timestamp()]
    recent_senders = recent_messages.sender_id.drop_duplicates().tolist()

    long_term_idle = set()

    # Record long-term idle status in zerver_userprofile
    for user_profile_row in zerver_userprofile:
        if user_profile_row['id'] in recent_senders:
            continue
        else:
            user_profile_row['long_term_idle'] = True
            # Setting last_active_message_id to 1 means the user, if
            # imported, will get the full message history for the
            # streams they were on.
            user_profile_row['last_active_message_id'] = 1
            long_term_idle.add(user_profile_row['id'])

    return long_term_idle

def convert_yammer_messages(yammer_data: Dict[str, pd.DataFrame], realm_id: int,
                            added_recipient: AddedRecipientsT, realm: ZerverFieldsT,
                            zerver_userprofile: List[ZerverFieldsT],
                            zerver_realmemoji: List[ZerverFieldsT], domain_name: str,
                            output_dir: str,
                            chunk_size: int=MESSAGE_BATCH_CHUNK_SIZE) -> Tuple[List[ZerverFieldsT],
                                                                               List[ZerverFieldsT],
                                                                               List[ZerverFieldsT]]:
    """
    Returns:
    1. reactions, which is a list of the reactions (none in case of Yammer)
    2. uploads, which is a list of uploads to be mapped in uploads records.json
    3. attachment, which is a list of the attachments
    """
    long_term_idle = process_long_term_idle_users(yammer_data, zerver_userprofile)

    # Now, we actually import the messages.
    all_messages = yammer_data['Messages']
    logging.info('######### IMPORTING MESSAGES STARTED #########\n')

    total_reactions = []  # type: List[ZerverFieldsT]
    total_attachments = []  # type: List[ZerverFieldsT]
    total_uploads = []  # type: List[ZerverFieldsT]

    # The messages are stored in batches
    dump_file_id = 1

    subscriber_map = make_subscriber_map(
        zerver_subscription=realm['zerver_subscription'],
    )

    zerver_messages, zerver_usermessages, attachments, uploads, reactions = \
                    yammer_message_to_zerver_message(yammer_data, realm_id, realm,
                                                     added_recipient, all_messages,
                                                     zerver_realmemoji, subscriber_map,
                                                     domain_name, long_term_idle)

    while len(zerver_messages) > 0 or len(zerver_usermessages) > 0:
        if len(zerver_messages) > 0:
            try:
                zerver_chunk = zerver_messages[0:MESSAGE_BATCH_CHUNK_SIZE-1]
                del zerver_messages[0:MESSAGE_BATCH_CHUNK_SIZE-1]
            except ValueError:
                zerver_chunk = zerver_messages[0:]
                del zerver_messages[:]
        else:
            zerver_chunk = []

        if len(zerver_usermessages) > 0:
            try:
                zerver_userchunk = zerver_usermessages[0:MESSAGE_BATCH_CHUNK_SIZE-1]
                del zerver_usermessages[0:MESSAGE_BATCH_CHUNK_SIZE-1]
            except ValueError:
                zerver_userchunk = zerver_usermessages[0:]
                del zerver_usermessages[:]
        else:
            zerver_userchunk = []

        message_json = dict(
            zerver_message=zerver_chunk,
            zerver_usermessage=zerver_userchunk)

        message_file = "/messages-%06d.json" % (dump_file_id,)
        logging.info("Writing Messages to %s\n" % (output_dir + message_file))
        create_converted_data_files(message_json, output_dir, message_file)
        dump_file_id += 1

    logging.info('######### IMPORTING MESSAGES FINISHED #########\n')
    return reactions, uploads, attachments

def yammer_message_to_zerver_message(yammer_data: Dict[str, pd.DataFrame],
                                     realm_id: int, realm: ZerverFieldsT,
                                     added_recipient: AddedRecipientsT,
                                     all_messages: pd.DataFrame,
                                     zerver_realmemoji: List[ZerverFieldsT],
                                     subscriber_map: Dict[int, Set[int]],
                                     domain_name: str,
                                     long_term_idle: Set[int]) -> Tuple[List[ZerverFieldsT],
                                                                        List[ZerverFieldsT],
                                                                        List[ZerverFieldsT],
                                                                        List[ZerverFieldsT],
                                                                        List[ZerverFieldsT]]:
    """
    Returns:
    1. zerver_message, which is a list of the messages
    2. zerver_usermessage, which is a list of the usermessages
    3. zerver_attachment, which is a list of the attachments
    4. uploads_list, which is a list of uploads to be mapped in uploads records.json
    5. reaction_list, which is a list of all user reactions
    """
    zerver_message = []
    zerver_usermessage = []  # type: List[ZerverFieldsT]
    uploads_list = []  # type: List[ZerverFieldsT]
    zerver_attachment = []  # type: List[ZerverFieldsT]
    reaction_list = []  # type: List[ZerverFieldsT]

    added_messages = {} # type Dict[str, ZerverFieldsT]

    total_user_messages = 0
    total_skipped_user_messages = 0
    for message in all_messages.itertuples():
        users = yammer_data['Users']

        if message.sender_id in users.index:
            user_id = int(users.zulip_id[message.sender_id])
        else:
            user_id = int(message.sender_id)

        if pd.notna(message.group_id):
            stream_name = "Yammer/" + str(yammer_data['Groups'].name[message.group_id])
            stream_id = int(yammer_data['Groups'].zulip_id[message.group_id])
        else:
            # FIXME: Should this really be hardcoded?
            stream_name = "Yammer/All Company"
            stream_id = 1

        recipient_id = added_recipient[stream_name]
        message_id = message.zulip_id

        try:
            content, mentioned_user_ids, has_link = convert_to_zulip_markdown(
                realm, users, message, added_messages)
        except Exception:
            print("Yammer message unexpectedly missing text representation:")
            print(message)
            raise

        rendered_content = None

        file_info = process_message_files(
            yammer_data=yammer_data,
            message=message,
            domain_name=domain_name,
            realm_id=realm_id,
            message_id=message_id,
            zerver_attachment=zerver_attachment,
            uploads_list=uploads_list,
        )

        content += file_info['content']
        has_link = has_link or file_info['has_link']

        has_attachment = file_info['has_attachment']
        has_image = file_info['has_image']

        # construct message
        topic_name = 'Yammer: ' + str(message.thread_id)

        zulip_message = build_message(topic_name, message.created_at, message_id,
                                      content, rendered_content, user_id, recipient_id,
                                      has_image, has_link, has_attachment)

        if pd.notna(message.replied_to_id):
            # Zulip doesn't provide real threading, so we add a link
            # to the replied-to message where necessary. The code is
            # similar to ``near_stream_message_url()`` but creates
            # relative links.
            if message.replied_to_id in added_messages:
#                replied_to_header = "(go to message being replied to)\n\n"
                replied_to_msg = added_messages[message.replied_to_id]
                encoded_topic = hash_util_encode(topic_name)
                encoded_stream = encode_stream(stream_id=stream_id, stream_name=stream_name)

                replied_to_link = '/'.join(['#narrow',
                                            'stream',
                                            encoded_stream,
                                            'topic',
                                            encoded_topic,
                                            'near',
                                            str(replied_to_msg['id']),
                                            ])
                replied_to_header = "[(go to message being replied to)](" \
                                    + replied_to_link + ")\n\n"
            else:
                replied_to_header = "(unable to locate message being replied to)\n\n"

            zulip_message['content'] = replied_to_header + zulip_message['content']

        zerver_message.append(zulip_message)
        added_messages[message.id] = zulip_message

        # construct usermessages
        (num_created, num_skipped) = build_usermessages(
            zerver_usermessage=zerver_usermessage,
            subscriber_map=subscriber_map,
            recipient_id=recipient_id,
            mentioned_user_ids=mentioned_user_ids,
            message_id=message_id,
            long_term_idle=long_term_idle,
        )
        total_user_messages += num_created
        total_skipped_user_messages += num_skipped

    logging.debug("Created %s UserMessages; deferred %s due to long-term idle" % (
        total_user_messages, total_skipped_user_messages))
    return zerver_message, zerver_usermessage, zerver_attachment, uploads_list, \
        reaction_list

def process_message_files(yammer_data: Dict[str, pd.DataFrame],
                          message: namedtuple,
                          domain_name: str,
                          realm_id: int,
                          message_id: int,
                          zerver_attachment: List[ZerverFieldsT],
                          uploads_list: List[ZerverFieldsT]) -> Dict[str, Any]:
    has_attachment = False
    has_image = False
    has_link = False

    mimetypes.init()

    files = yammer_data['Files']


    markdown_links = []

    ATTACHMENT_REGEX = r"(" + "|".join([r"(uploadedfile):(?P<file_id>\d+)",
                                        r"(opengraphobject):(\d+)",
                                        r"(opengraphobject):\[(\d+)\s*:\s*(?P<ogobj_uri>.*?)\]",
                                        ]) + r")"

    if pd.notna(message.attachments):
        for m in re.finditer(ATTACHMENT_REGEX, message.attachments):
            if m.group('file_id'):
                # file
                file_id = m.group('file_id')
                file_data = files[files.file_id == np.int64(file_id)].iloc[0]


                if pd.isna(file_data["deleted_at"]):
                    try:
                        zip_info = file_data["zip_file"].getinfo(file_data["path"])
                    except KeyError:
                        zip_info = None

                    if zip_info:
                        fileinfo = dict(
                            name=file_data["name"],
                            title=(file_data["description"] if pd.notna(file_data["description"]) \
                                   else file_data["name"]),
                            size=zip_info.file_size,
                            created=float(file_data["uploaded_at"]),
                            )

                        has_attachment = True
                        has_link = True

                        if mimetypes.guess_type(fileinfo["name"])[0] == 'image':
                            has_image = True

                        s3_path, content = get_attachment_path_and_content(fileinfo, realm_id)
                        markdown_links.append(content)

                        try:
                            user_profile_id = int(yammer_data['Users'].loc[message.sender_id].zulip_id)
                        except KeyError:
                            user_profile_id = int(message.sender_id)
                            raise

                        upload = dict(zip_file=file_data["zip_file"], # ZIP file object from which to extract
                                      path=file_data["path"], # ZIP member to extract
                                      realm_id=realm_id,
                                      content_type=None,
                                      user_profile_id=user_profile_id,
                                      last_modified=fileinfo["created"],
                                      user_profile_email=message.sender_email,
                                      s3_path=s3_path, # File name to extract to
                                      size=fileinfo["size"]
                                      )
                        uploads_list.append(upload)

                        build_attachment(realm_id, {message.zulip_id}, user_profile_id,
                                         fileinfo, s3_path, zerver_attachment)
                    else:
                        markdown_links.append("*File %s couldn't be imported*" % file_data.name)
                else:
                    markdown_links.append("*File %s was deleted*" % file_data.name)

            elif m.group('ogobj_uri'):
                # link
                has_link = True
                markdown_links.append(m.group('ogobj_uri'))

    content = "\n" + "\n".join(markdown_links) if markdown_links else ""
    if message.body == "" and content == "":
        content = str("*(This message was originally empty. It may have contained "
                      "a link attachment which couldn't be reconstructed.)*\n")

    return dict(
        content=content,
        has_attachment=has_attachment,
        has_image=has_image,
        has_link=has_link,
    )

def get_attachment_path_and_content(fileinfo: ZerverFieldsT, realm_id: int) -> Tuple[str,
                                                                                     str]:
    # Should be kept in sync with its equivalent in zerver/lib/uploads in the function
    # 'upload_message_file'
    s3_path = "/".join([
        str(realm_id),
        'YammerImportAttachment',  # This is a special placeholder which should be kept
                                   # in sync with 'exports.py' function 'import_message_data'
        format(random.randint(0, 255), 'x'),
        random_name(18),
        sanitize_name(fileinfo['name'])
    ])
    attachment_path = ('/user_uploads/%s' % (s3_path))
    content = '\n[%s](%s)' % (fileinfo['title'], attachment_path)

    return s3_path, content

def read_dumps(ymdumps, tables):
    """
    Read Yammer export data (ZIP archives containing CSV and media
    attachments) passed into `ymdumps` as a list of file names and
    append to the dictionary `tables`. The archives will be processed
    in order starting at index 0.
    """

    def parse_time_str(t):
        """
        Convert date specification into `datetime` object

        Returns `numpy.nan` if `t` is empty.
        """
        return dateutil.parser.parse(t).timestamp() if t else np.nan

    def msg_body_to_str(n):
        """
        Convert message body to string

        If message body is already a string, pass-through, else return
        empty string.
        """
        if isinstance(n, str):
            return n
        else:
            return ""

    for yd in ymdumps:
        logging.info("Reading %s\n" % yd)
        zf = zipfile.ZipFile(yd, 'r')
        zl = [ PurePath(x) for x in zf.namelist() ]
        for zp in zl:
            if zp.suffix == '.csv':
                converters = {}
                if zp.stem == 'Files':
                    converters={ 'uploaded_at': parse_time_str,
                                 'deleted_at': parse_time_str }
                elif zp.stem == 'Groups':
                    converters={ 'created_at': parse_time_str,
                                 'updated_at': parse_time_str }
                elif zp.stem == 'Messages':
                    converters={ 'created_at': parse_time_str,
                                 'deleted_at': parse_time_str,
                                 'body': msg_body_to_str}
                elif zp.stem == 'Networks':
                    converters={ 'created_at': parse_time_str }
                elif zp.stem == 'Topics':
                    converters={ 'created_at': parse_time_str }
                elif zp.stem == 'Users':
                    converters={ 'joined_at': parse_time_str,
                                 'deleted_at': parse_time_str }
                tbl = pd.read_csv(zf.open(str(zp)), converters=converters)
                if zp.stem == 'Files':
                   # Add column for accessing file attachments later
                   tbl = tbl.assign(zip_file=zf)
                if not zp.stem in tables:
                    tables[zp.stem] = []
                tables[zp.stem].append(tbl)

def concat_tables(tables):
    """
    Concatenate data sets from different dump files, removing
    duplicate indices identified by their common column label *id*
    (observed in e.g. 'Users.csv').
    """
    for k,v in tables.items():
        tables[k] = pd.concat(tables[k]).drop_duplicates(subset=['id'],
                                                          keep='last')

def reassign_index(tables):
    """
    Create new ID mapping between Yammer and Zulip

    This adds a column *zulip_id*.
    """
    #  Messages in particular need time-sorted ID.
    tables['Messages'] = tables['Messages'].sort_values(by='created_at')

    for k,v in tables.items():
        tables[k].set_index('id', False, False, True, False)
        # Add sequence numbers starting from 1, except in case of
        # groups (Zulip streams) which have to start at 2 due to
        # implicit "All Company".
        if k == 'Groups':
            start_id = 2
        else:
            start_id = 1
        tables[k] = v.assign(zulip_id=range(start_id, v.count(axis=0).id + start_id))

def do_convert_data(yammer_zip_files: List[str], output_dir: str, threads: int=6) -> None:
    # Subdomain is set by the user while running the import command
    realm_subdomain = ""

    yammer_data_dir = "yammer_data"
    if not os.path.exists(yammer_data_dir):
        os.makedirs(yammer_data_dir)

    tables = {}
    read_dumps(yammer_zip_files, tables)
    concat_tables(tables)
    reassign_index(tables)

    realm_id = int(tables['Networks'].iloc[0].zulip_id)
    domain_name = tables['Networks'].iloc[0].permalink

    # Limit to users who actually participated in conversations,
    # i.e. ignore test accounts, etc.
    tables['Users'] = tables['Users'][tables['Users'].index.isin(tables['Messages']['sender_id'])]

    realm, added_users, added_recipient, avatar_list, \
        emoji_url_map = yammer_network_to_realm(domain_name, realm_id,
                                                realm_subdomain, tables)

    reactions, uploads_list, zerver_attachment = convert_yammer_messages(
        tables, realm_id, added_recipient, realm, realm['zerver_userprofile'],
        realm['zerver_realmemoji'], domain_name, output_dir)

    emoji_records = {}
    avatar_records = {}

    uploads_folder = os.path.join(output_dir, 'uploads')
    os.makedirs(os.path.join(uploads_folder, str(realm_id)), exist_ok=True)
    uploads_records = yammer_process_uploads(uploads_list, uploads_folder)
    attachment = {"zerver_attachment": zerver_attachment}

    # IO realm.json
    create_converted_data_files(realm, output_dir, '/realm.json')
    # IO emoji records
    create_converted_data_files(emoji_records, output_dir, '/emoji/records.json')
    # IO avatar records
    create_converted_data_files(avatar_records, output_dir, '/avatars/records.json')
    # IO uploads records
    create_converted_data_files(uploads_records, output_dir, '/uploads/records.json')
    # IO attachments records
    create_converted_data_files(attachment, output_dir, '/attachment.json')

    # remove yammer dir
    rm_tree(yammer_data_dir)
    subprocess.check_call(["tar", "-czf", output_dir + '.tar.gz', output_dir, '-P'])

    logging.info('######### DATA CONVERSION FINISHED #########\n')
    logging.info("Zulip data dump created at %s" % (output_dir))

def yammer_process_uploads(upload_list: List[ZerverFieldsT], upload_dir: str) -> List[ZerverFieldsT]:
    """
    This function unpacks the uploads and saves it in the realm's upload directory.
    Required parameters:

    1. upload_list: List of uploads to be mapped in uploads records.json file
    2. upload_dir: Folder where the downloaded uploads are saved
    """
    logging.info('######### GETTING ATTACHMENTS #########\n')
    logging.info('UNPACKING ATTACHMENTS .......\n')

    for upload in upload_list:
        upload_s3_path = PurePath(upload_dir).joinpath(PurePath(upload['s3_path']))
        upload_s3_dir = str(upload_s3_path.parent)
        upload_s3_path = str(upload_s3_path)
        logging.info('%s -> %s\n' % (upload['path'], upload_s3_path))
        os.makedirs(upload_s3_dir)
        fo = open(upload_s3_path, 'wb')
        fo.write(upload['zip_file'].read(upload['path']))
        fo.close()
        del upload['zip_file']
        upload['path'] = upload_s3_path

    logging.info('######### GETTING ATTACHMENTS FINISHED #########\n')
    return upload_list
