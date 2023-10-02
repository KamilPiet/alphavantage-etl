import configparser
import git
import os
from datetime import datetime


def get_git_conf():
    """Get and return GitHub username and email address."""
    git_username, git_email = None, None
    env_var_git_username = str(os.getenv('AV_ETL_GIT_USERNAME'))
    env_var_git_email = str(os.getenv('AV_ETL_GIT_EMAIL'))
    gitconfig_path = os.path.expanduser('~/.gitconfig')
    config = configparser.ConfigParser()

    # Get the username from the env var if set
    if env_var_git_username != 'None' and env_var_git_username != '':
        git_username = env_var_git_username

    # Otherwise try to get the username from .gitconfig file in the default loaction
    else:
        try:
            config.read(gitconfig_path)
            git_username = config['user']['name']
        except FileNotFoundError as e:
            print(f'Config file not found: {e}')
        except KeyError as e:
            print(f'Key not found in the config file: {e}')

    # Get the email address from the env var if set
    if env_var_git_email != 'None' and env_var_git_email != '':
        git_email = env_var_git_email

    # Otherwise try to get the email address from .gitconfig file in the default loaction
    else:
        try:
            config.read(gitconfig_path)
            git_email = config['user']['email']
        except FileNotFoundError as e:
            print(f'Config file not found: {e}')
        except KeyError as e:
            print(f'Key not found in the config file: {e}')
    return git_username, git_email


def pull_from_remote(report_path, remote_url, remote_name):
    try:
        # Check if the repository already exists locally
        repo = git.Repo(report_path)
        print('Pulling from the remote repository...')
        repo.create_remote(remote_name, remote_url)
        repo.git.pull('--set-upstream', remote_name, 'main')
        print('Pull complete.')

    except git.exc.NoSuchPathError:
        # If the repository doesn't exist locally, clone it and try to set username and email address in the config file
        print('Repository not found locally. Cloning the repository...')
        repo = git.Repo.clone_from(remote_url, report_path)
        print('Clone complete.')

        git_username, git_email = get_git_conf()
        if git_username is not None:
            repo.config_writer().set_value('user', 'name', git_username).release()
        else:
            print('Username not set.')
        if git_email is not None:
            repo.config_writer().set_value('user', 'email', git_email).release()
        else:
            print('Email not set.')

    # Delete the remote from the config file to avoid compromising the access token that is visible in the remote's URL
    repo.delete_remote(remote_name)


def push_to_remote(report_path, remote_url, remote_name):
    repo = git.Repo(report_path)
    print('Staging "index.html"...')
    repo.index.add(os.path.join(report_path, 'index.html'))
    print('Commiting staged files...')
    repo.index.commit(f'{datetime.today().strftime("%d-%m-%Y")} price report update')
    print('Pushing to remote...')
    repo.create_remote(remote_name, remote_url)
    repo.git.push('--set-upstream', remote_name, 'main')

    # Delete the remote from the config file to avoid compromising the access token that is visible in the remote's URL
    repo.delete_remote(remote_name)
    print('Push complete.')


def publish_report(report, working_dir_path):
    """Publish the updated report on GitHub Pages."""
    report_path = os.path.join(working_dir_path, 'report/')
    remote_name = 'origin'
    github_token = os.getenv('AV_ETL_GITHUB_TOKEN')
    repo_url = os.getenv('AV_ETL_REMOTE_REPO')

    # Add "HTTPS" if missing
    if repo_url[:4].upper() != 'HTTP':
        repo_url = 'HTTPS://'.join(repo_url)

    # Insert the access token into the URL
    i = repo_url.index(':') + 3
    remote_url = repo_url[:i] + github_token + '@' + repo_url[i:]

    # Prepare the local repository and push the updated report to GitHub Pages repository
    pull_from_remote(report_path, remote_url, remote_name)
    report.save(path=os.path.join(report_path, 'index.html'))
    push_to_remote(report_path, remote_url, remote_name)
