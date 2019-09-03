""" Convert Redmine objects to gitlab's
"""

import logging
import yaml

log = logging.getLogger(__name__)
user_dict = None

# TODO: make configurable
ANONYMOUS_USERNAME = ''

# Utils

def load_user_dict(path):
    global user_dict
    with open(path, 'r') as stream:
        user_dict = yaml.load(stream)

def redmine_username_to_gitlab_username(redmine_username):
    if user_dict is not None and redmine_username in user_dict:
        return user_dict[redmine_username]
    return redmine_username


def redmine_uid_to_gitlab_user(redmine_id, redmine_user_index, gitlab_user_index):
    redmine_login = redmine_user_index[redmine_id]['login']

    # Check if mapping for this user exists
    redmine_login = redmine_username_to_gitlab_username(redmine_login)

    if not redmine_login in gitlab_user_index:
        redmine_login = 'root'
    return gitlab_user_index[redmine_login]


def convert_attachment(redmine_issue_attachment, redmine_api_key):
    """ Convert a list of redmine attachments to gitlab uploads

    :param redmine_issue_attachment: a dict describing redmine-api-style attachment
    :param redmine_api_key: the redmine api key used for getting the attachment without logging in
    :return: a dict describing the attachment
    """
    uploads = {
        'filename': redmine_issue_attachment['filename'],
        'description': redmine_issue_attachment.get('description'),
        'content_url': '{}?key={}'.format(redmine_issue_attachment['content_url'], redmine_api_key),
        'content_type': redmine_issue_attachment.get('content_type', 'application/octet-stream')
    }

    return uploads


def convert_notes(redmine_issue_journals, redmine_user_index, gitlab_user_index, textile_converter, sudo):
    """ Convert a list of redmine journal entries to gitlab notes

    Filters out the empty notes (ex: bare status change)
    Adds metadata as comment

    :param redmine_issue_journals: list of redmine "journals"
    :return: yielded couple ``data``, ``meta``. ``data`` is the API payload for
        an issue note and meta a dict (containing, at the moment, only a
        "sudo_user" key).
    """

    for entry in redmine_issue_journals:
        journal_notes = entry.get('notes', '')

        if journal_notes is None:
            continue

        if len(journal_notes) > 0:
            journal_notes = textile_converter.convert(journal_notes)
            try:
                author = redmine_uid_to_gitlab_user(
                    entry['user']['id'], redmine_user_index, gitlab_user_index)['username']
                author_name = entry['user']['name']
                gitlab_author_found = True
            except KeyError:
                user = entry.get('user', None)
                author_name = user['name'] if user is not None else user

                # In some cases you have anonymous notes, which do not exist in gitlab.
                log.warning(
                    'Redmine user "{}" is unknown, attribute note to {}'.format(
                        author_name,
                        ANONYMOUS_USERNAME if ANONYMOUS_USERNAME else 'current admin'
                    )
                )
                author = ANONYMOUS_USERNAME
                gitlab_author_found = False

            if not sudo or not gitlab_author_found:
                creator_text = " by {}".format(author_name)
            else:
                creator_text = ''
            body = "{}\n\n*(from redmine: written on {}{})*".format(
                journal_notes, entry['created_on'][:10], creator_text)
            data = {'body': body, 'created_at': entry['created_on']}
            if sudo:
                meta = {'sudo_user': author}
            else:
                meta = {}
            yield (data, meta)


def relations_to_string(relations, children, parent_id, issue_id):
    """ Convert redmine formal relations to some denormalized string

    That's the way gitlab does relations, by "mentioning".

    :param relations: list of issues relations
    :param issue_id: the current issue id
    :return a string listing relations.
    """
    l = []
    for i in relations:
        if issue_id == i['issue_id']:
            other_issue_id = i['issue_to_id']
        else:
            other_issue_id = i['issue_id']
        l.append('  * {} #{}'.format(i['relation_type'], other_issue_id))

    for i in children:
        id = i['id']
        l.append('  * {} #{}'.format('child', id))

    if parent_id > 0:
       l.append('  * {} #{}'.format('parent', parent_id))

    return "\n".join(l)

def changesets_to_string(changesets):
    """ Convert redmine formal changesets to some denormalized string

    :param changesets: list of issues changesets
    :return a string listing changesets.
    """
    l = []
    for i in changesets:
        revision = i['revision']
        committed_on = i['committed_on']
        comments = i['comments']
        try:
            user = i['user']['name']
            by_user_str = ' by {}'.format(user)
        except KeyError:
            by_user_str = ''

        l.append('  * Revision {}{} on {}:\n\n```\n{}\n```\n'.format(
            revision, by_user_str, committed_on, comments))

    return "\n".join(l)

def custom_fields_to_string(custom_fields, custom_fields_include):
    """ Convert redmine custom fields to some denormalized string

    :param custom_fields: list of issues custom_fields
    :return a string listing custom_fileds.
    """
    l = []
    for i in custom_fields:
        name = i['name']

        if name in custom_fields_include and i.get('value'):
            # Name: Value
            l.append('  * {}: {}'.format(name, i['value']))

    return "\n".join(l)

# Convertor

def convert_issue(redmine_api_key, redmine_issue, redmine_user_index, gitlab_user_index,
		  gitlab_milestones_index, closed_states, custom_fields_include, textile_converter, keep_title, sudo):

    issue_state = redmine_issue['status']['name']

    if redmine_issue.get('closed_on', None):
        # quick'n dirty extract date
        close_text = ', closed on {}'.format(redmine_issue['closed_on'][:10])
        closed = True
    elif issue_state.lower() in closed_states:
        close_text = ', closed (state: {})'.format(issue_state)
        closed = True
    else:
        close_text = ''
        closed = False

    relations = redmine_issue.get('relations', [])
    children = redmine_issue.get('children', [])
    parent_id = 0
    if redmine_issue.get('parent', None):
        parent_id = redmine_issue['parent']['id']

    relations_text = relations_to_string(relations, children, parent_id, redmine_issue['id'])
    if len(relations_text) > 0:
        relations_text = "\n* Relations:\n" + relations_text

    changesets = redmine_issue.get('changesets', [])
    changesets_text = changesets_to_string(changesets)
    if len(changesets_text) > 0:
        changesets_text = "\n* Changesets:\n" + changesets_text

    custom_fields = redmine_issue.get('custom_fields', [])
    custom_fields_text = custom_fields_to_string(custom_fields, custom_fields_include)
    if len(custom_fields_text) > 0:
        custom_fields_text = "\n* Custom Fields:\n" + custom_fields_text

    labels = [redmine_issue['tracker']['name']]
    if (redmine_issue.get('category')):
        labels.append(redmine_issue['category']['name'])
    if (redmine_issue.get('status')):
        labels.append(redmine_issue['status']['name'])
    if (redmine_issue.get('priority')):
        labels.append(redmine_issue['priority']['name'])

    attachments = redmine_issue.get('attachments', [])
    due_date = redmine_issue.get('due_date', None)
    if keep_title:
        title = redmine_issue['subject']
    else:
        title = '-RM-{}-MR-{}'.format(redmine_issue['id'], redmine_issue['subject'])

    try:
        author_login = redmine_uid_to_gitlab_user(
            redmine_issue['author']['id'], redmine_user_index, gitlab_user_index)['username']
        gitlab_author_found = True

    except KeyError:
        log.warning(
            'Redmine issue #{} author {} is unknown, gitlab issue is attributed to {}'.format(
                redmine_issue['id'],
                redmine_issue['author']['name'],
                ANONYMOUS_USERNAME if ANONYMOUS_USERNAME else 'current admin'
            )
        )
        author_login = ANONYMOUS_USERNAME
        gitlab_author_found = False

    if not sudo or not gitlab_author_found:
        creator_text = ' by {}'.format(redmine_issue['author']['name'])
    else:
        creator_text = ''

    description = redmine_issue.get('description', "")
    data = {
        'title': title,
        'description': '{}\n\n*(from redmine: issue id {}, created on {}{}{})*\n{}{}{}'.format(
            textile_converter.convert(description),
            redmine_issue['id'],
            redmine_issue['created_on'][:10],
            creator_text,
            close_text,
            relations_text,
            changesets_text,
            custom_fields_text
        ),
        'due_date': due_date,
        'created_at': redmine_issue['created_on'],
        'labels': ','.join(labels),
    }

    version = redmine_issue.get('fixed_version', None)
    if version:
        data['milestone_id'] = gitlab_milestones_index[version['name']]['id']

    meta = {
        'notes': list(convert_notes(redmine_issue['journals'],
                          redmine_user_index, gitlab_user_index, textile_converter, sudo)),
        'must_close': closed,
        'uploads': list(convert_attachment(a, redmine_api_key) for a in attachments)
    }
    if sudo:
        meta['sudo_user'] = author_login

    assigned_to = redmine_issue.get('assigned_to', None)
    if assigned_to is not None:
        try:
            data['assignee_id'] = redmine_uid_to_gitlab_user(
                assigned_to['id'], redmine_user_index, gitlab_user_index)['id']
        except KeyError:
            # add original assignee as label
            log.warning('Redmine issue #{} assignee is unknown, adding label "{}"'.format(redmine_issue['id'], assigned_to['name']))
            data['labels'] = data['labels'] + ',' + assigned_to['name']

    return data, meta, redmine_issue['id']


def convert_version(redmine_version):
    """ Turns a redmine version into a gitlab milestone

    Do not handle the issues linked to the milestone/version.
    Note that redmine do not expose a due date in API.

    :param redmine_version: a dict describing redmine-api-style version
    :rtype: couple: dict, dict
    :return: a dict describing gitlab-api-style milestone and another for meta
    """
    milestone = {
        "title": redmine_version['name'],
        "description": '{}\n\n*(from redmine: created on {})*'.format(
            redmine_version.get('description', ""),
            redmine_version['created_on'][:10])
    }
    if 'due_date' in redmine_version:
        milestone['due_date'] = redmine_version['due_date'][:10]

    must_close = redmine_version['status'] == 'closed'

    return milestone, {'must_close': must_close}