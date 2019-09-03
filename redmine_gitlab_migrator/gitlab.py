import re
import logging
import requests
import urllib.parse

from . import APIClient, Project
from urllib.request import urlopen

from redmine_gitlab_migrator.converters import redmine_username_to_gitlab_username

from json.decoder import JSONDecodeError

log = logging.getLogger(__name__)

class GitlabClient(APIClient):
    # see http://doc.gitlab.com/ce/api/#pagination
    MAX_PER_PAGE = 100

    def get(self, *args, **kwargs):
        kwargs['params'] = kwargs.get('params', {})
        kwargs['params']['page'] = 1
        kwargs['params']['per_page'] = self.MAX_PER_PAGE

        result = super().get(*args, **kwargs)
        while (len(result) > 0 and len(result) % self.MAX_PER_PAGE == 0):
            kwargs['params']['page'] += 1
            result.extend(super().get(*args, **kwargs))
        return result

    def get_auth_headers(self):
        return {"PRIVATE-TOKEN": self.api_key}

    def check_is_admin(self):
        pass


class GitlabInstance:
    def __init__(self, url, client):
        self.url = url.strip('/')  # normalize URL
        self.api = client

    def get_all_users(self):
        return self.api.get('{}/users'.format(self.url))

    def get_users_index(self):
        """ Returns dict index of users (by login)
        """
        return {i['username']: i for i in self.get_all_users()}

    def get_group_members(self, group_id):
        return self.api.get('{}/groups/{}/members'.format(self.url, group_id))


    def check_users_exist(self, usernames):
        """ Returns True if all users exist
        """
        gitlab_user_names = set([i['username'] for i in self.get_all_users()])

        translated = []
        for i in usernames:
            print(i, redmine_username_to_gitlab_username(i))
            translated.append(redmine_username_to_gitlab_username(i))
        return all((i in gitlab_user_names for i in translated))


class GitlabProject(Project):
    REGEX_PROJECT_URL = re.compile(
        r'^(?P<base_url>https?://[^/]+/)(?P<namespace>[\.\w\._/-]+)/(?P<project_name>[\w\._-]+)$')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_id = None

        self.instance_url = '{}/api/v4'.format(
            self._url_match.group('base_url'))

        # fetch project_id via api, thanks to lewicki-pk
        # https://github.com/oasiswork/redmine-gitlab-migrator/pull/2
        # but also take int account, that there might be the same project in different namespaces
        path_with_namespace = (
            '{namespace}/{project_name}'.format(
                **self._url_match.groupdict()))
        projectId = -1
        groupId = None

        project_info = self.api.get('{}/projects/{}'.format(self.instance_url,urllib.parse.quote_plus(path_with_namespace)))

        projectId = project_info.get('id')
        if project_info.get('namespace').get('kind') == 'group':
            groupId = project_info.get('namespace').get('id')

        self.project_id = projectId
        if projectId == -1 :
            raise ValueError('Could not get project_id for path_with_namespace: {}'.format(path_with_namespace))
        if groupId:
            self.group_id = groupId

        self.api_url = (
            '{base_url}api/v4/projects/'.format(
                **self._url_match.groupdict())) + str(projectId)
        
        self.group_api_url = (
            '{base_url}api/v4/groups/'.format(
                **self._url_match.groupdict())) + str(groupId)


    def is_repository_empty(self):
        """ Heuristic to check if repository is empty
        """
        return self.api.get(self.api_url)['default_branch'] is None

    def uploads_to_string(self, uploads):

        uploads_url = '{}/uploads'.format(self.api_url)
        l = []
        for u in uploads:

           log.info('\tuploading {} ({} / {})'.format(u['filename'], u['content_url'], u['content_type']))

           try:
               # http://docs.python-requests.org/en/latest/user/quickstart/#post-a-multipart-encoded-file
               # http://stackoverflow.com/questions/20830551/how-to-streaming-upload-with-python-requests-module-include-file-and-data
               files = [("file", (u['filename'], urlopen(u['content_url']), u['content_type']))]
           except urllib.error.HTTPError as e:
               if e.code == 404:
                   # attachment was not found in redmine
                   l.append('{} {}'.format('(attachment did not exist in redmine)', u['description']))
                   continue

           try:
               upload = self.api.post(
                   uploads_url, files=files)
           except requests.exceptions.HTTPError:
               # gitlab might throw an "ArgumentError (invalid byte sequence in UTF-8)" in production.log
               # if the filename contains special chars like german "umlaute"
               # in that case we retry with an ascii only filename.
               files = [("file", (self.remove_non_ascii(u['filename']), urlopen(u['content_url']), u['content_type']))]
               upload = self.api.post(
                   uploads_url, files=files)

           l.append('{} {}'.format(upload['markdown'], u['description']))

        return "\n  * ".join(l)

    def remove_non_ascii(self, text):
        # http://stackoverflow.com/a/20078869/98491
        return ''.join([i if ord(i) < 128 else ' ' for i in text])

    def create_issue(self, data, meta):
        """ High-level issue creation

        :param meta: dict with "sudo_user", "must_close", "notes" and "attachments" keys
        :param data: dict formatted as the gitlab API expects it
        :return: the created issue (without notes)
        """

        # attachments have to be uploaded prior to creating an issue
        # attachments are not related to an issue but can be referenced instead
        # see: https://docs.gitlab.com/ce/api/projects.html#upload-a-file
        uploads_text = self.uploads_to_string(meta['uploads'])
        if len(uploads_text) > 0:
           data['description'] = "{}\n* Uploads:\n  * {}".format(data['description'], uploads_text)

        headers = {}
        if 'sudo_user' in meta:
            headers['SUDO'] = meta['sudo_user']
        issues_url = '{}/issues'.format(self.api_url)

        try:
            issue = self.api.post(
                issues_url, data=data, headers=headers)
            # print("ISSUE-DICT: ", issue)  - Example -  {'id': 1376, 'iid': 10, 'project_id': 13, 'title': '-RM-38-MR-Suchfunktion in Header: Umlaute ergeben 0 Treffer wg encoding', 'description': 'Für Ihre Suchanfrage - %C3%B6ffnungszeiten - wurde keine Übereinstimmung\ngefunden\n\n\n*(from redmine: issue id 38, created on 2012-09-27, closed on 2012-09-27)*', 'state': 'opened', 'created_at': '2012-09-27T08:20:07.000Z', 'updated_at': '2019-08-29T19:40:59.818Z', 'closed_at': None, 'closed_by': None, 'labels': ['Fehler', 'Fertig (aktiv auf Echtsystem)', 'Hoch'], 'milestone': None, 'assignees': [], 'author': {'id': 3, 'name': 'Heiko Weier', 'username': 'Heiko', 'state': 'active', 'avatar_url': 'https://www.gravatar.com/avatar/01878d30f3854bfea39caad9beab738e?s=80&d=identicon', 'web_url': 'http://cls3.tub.tuhh.de/Heiko'}, 'assignee': None, 'user_notes_count': 0, 'merge_requests_count': 0, 'upvotes': 0, 'downvotes': 0, 'due_date': None, 'confidential': False, 'discussion_locked': None, 'web_url': 'http://cls3.tub.tuhh.de/crk0771/blub/issues/10', 'time_stats': {'time_estimate': 0, 'total_time_spent': 0, 'human_time_estimate': None, 'human_total_time_spent': None}, 'task_completion_status': {'count': 0, 'completed_count': 0}, 'has_tasks': False, '_links': {'self': 'http://cls3.tub.tuhh.de/api/v4/projects/13/issues/10', 'notes': 'http://cls3.tub.tuhh.de/api/v4/projects/13/issues/10/notes', 'award_emoji': 'http://cls3.tub.tuhh.de/api/v4/projects/13/issues/10/award_emoji', 'project': 'http://cls3.tub.tuhh.de/api/v4/projects/13'}, 'subscribed': True}
        except:
            log.error('Creating issue "{}" failed'.format(data['title']))
            #raise CommandError('Creating issue "{}" failed'.format(data['title']))

        # Handle issues notes
        try:
            issue_url = '{}/{}'.format(issues_url, issue['iid'])
            issue_notes_url = '{}/notes'.format(issue_url, 'notes')
        except:
            log.error('Creating note url failed subsequently to creating issue "{}" failed'.format(data['title']))
        for note_data, note_meta in meta['notes']:
            note_headers = {}
            try:
                if 'sudo_user' in note_meta:
                    note_headers['SUDO'] = note_meta['sudo_user']
                self.api.post(
                    issue_notes_url, data=note_data,
                    headers=note_headers)
            except:
                log.error('Adding note for issue "{}" failed'.format(data['title']))

        # Handle closed status
        try:
            if meta['must_close']:
                self.api.put(issue_url, {'state_event': 'close'})
        except:
            log.error('Setting closed status failed "{}" failed'.format(data['title']))

        # Ignore failed try blok above, keep going
        if 'issue' not in locals():
            issue = {'id': 0, 'iid': 0, 'title': data['title']}

        return issue

    def delete_issue(self, iid):
        issue_url = '{}/issues/{}'.format(self.api_url, iid)
        try:
            self.api.delete(issue_url)
        except JSONDecodeError:
            True

    def create_milestone(self, data, meta):
        """ High-level milestone creation

        :param meta: dict with "should_close"
        :param data: dict formatted as the gitlab API expects it
        :return: the created milestone
        """
        milestones_url = '{}/milestones'.format(self.api_url)

        # create milestone if not exists
        try:
            milestone = self.get_milestone_by_title(data['title'])
        except ValueError:
            milestone = self.api.post(milestones_url, data=data)

        if (meta['must_close'] and milestone['state'] != 'closed'):
            milestone_url = '{}/{}'.format(milestones_url, milestone['id'])
            altered_milestone = milestone.copy()
            altered_milestone['state_event'] = 'close'

            self.api.put(milestone_url, data=altered_milestone)
        return milestone

    def get_issues(self):
        return self.api.get('{}/issues'.format(self.api_url))

    def get_members(self):
        project_members = self.api.get('{}/members/all'.format(self.api_url))
        return project_members

    def get_members_index(self):
        """ Returns dict index of users (by login)
        """
        return {i['username']: i for i in self.get_members()}

    def get_milestones(self):
        if not hasattr(self, '_cache_milestones'):
            self._cache_milestones = self.api.get(
                '{}/milestones'.format(self.api_url))
        return self._cache_milestones

    def get_milestones_index(self):
        return {i['title']: i for i in self.get_milestones()}

    def get_milestone_by_id(self, _id):
        milestones = self.get_milestones()
        for i in milestones:
            if i['id'] == _id:
                return i
        raise ValueError('Could not get milestone for id {}'.format(_id))

    def get_milestone_by_title(self, _title):
        milestones = self.get_milestones()
        for i in milestones:
            if i['title'] == _title:
                return i
        raise ValueError('Could not get milestone for title {}'.format(_title))

    def has_members(self, usernames):
        gitlab_user_names = set([i['username'] for i in self.get_members()])
        return all((i in gitlab_user_names for i in usernames))

    def get_id(self):
        return self.api.get(self.api_url)['id']

    def get_instance(self):
        """ Return a GitlabInstance
        """
        return GitlabInstance(self.instance_url, self.api)

