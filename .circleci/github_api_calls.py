import argparse
import json
import logging
import sys
import requests
import re
import pendulum

base_url = "https://api.github.com"

available_commands = {
    "add_comment": [
        "Adds a single comment for a specified pull request.",
        "Required parameters: organization, repository, token, pull-request, extras[message]",
    ],
    "add_labels": [
        "Adds a set of labels for a specified pull request.",
        "Required parameters: organization, repository, token, pull-request, extras[labels]",
    ],
    "close_issue": [
        "Marks a specified issue as closed.",
        "Required parameters: organization, repository, token, extras[issue]",
    ],
    "delete_labels": [
        "Deletes a set of labels for a specified pull request.",
        "Required parameters: organization, repository, token, pull-request, extras[labels]",
    ],
    "dimiss_single_review": [
        "Dismisses a specific review for a specified pull request.",
        "Required parameters: organization, repository, token, pull-request, extras[review_id]",
    ],
    "dismiss_all_reviews": [
        "Dismisses all reviews for a specified pull request.",
        "Required parameters: organization, repository, token, pull-request",
    ],
    "get_commit_message": [
        "Gets a commit message, using the commit_id.",
        "Required parameters: organization, repository, token, pull-request, extras[commit_id]",
    ],
    "get_deploy_issue_number": [
        "Parses for a Github issue titled 'Deploy Request: YYYY-MM-DD', and returns the associated issue id.",
        "Required parameters: organization, repository, token",
    ],
    "get_prs_to_deploy": [
        "Parses for a Github issue titled 'Deploy Request: YYYY-MM-DD', and generates a list of mentioned PRs (in order of closed_at).",
        "Required parameters: organization, repository, token",
    ],
    "label_merged_pr": [
        "Adds and/or deletes a set of labels to a pull_request merged into develop or main.",
        "Required parameters: organization, repository, token, pull-request, extras[commit_id, labels_to_add, labels_to_delete]",
    ],
    "label_prs_mentioned_in_commits": [
        "Adds and/or deletes a set of labels to all PRs mentioned in the commit messages of specified pull_request.",
        "Required parameters: organization, repository, token, pull-request, extras[commit_id, labels_to_add, labels_to_delete]",
    ],
    "list_commits": [
        "Fetches a list of commits for a specified pull request.",
        "Required parameters: organization, repository, token, pull-request",
    ],
    "list_deleted_files": [
        "Fetches a list of deleted files for a specific commit.",
        "Required parameters: organization, repository, token, extras[commit_id]",
    ],
    "open_pr": [
        "Opens a PR, using the supplied head branch into base branch.",
        "Required parameters: organization, repository, token, extras[head, base, title]",
    ],
}
command_template = 'Expected Syntax:\n\tpython3 github_api_call.py -o <Organization Name> -r <Repository> -t <O-Auth Token> -u <Github username> -p <Github password> -l <PR Number> -c <Github API Command> -e \'{"x": "sample", "y": 5, "z": "test}\'\n'


######################
#  HELPER FUNCTIONS  #
######################


def build_headers(token, username, password):
    """Format secret(s) for headers of API call."""
    if token:
        headers = {
            "Authorization": f"token { token }",
        }
    elif username and password:
        headers = {"Authorization": f"Basic { username }:{ password }"}
    else:
        raise Exception(
            "Either Authentication Token or Username + Password need to be included in request."
        )
    return headers


def format_epilog():
    """Print available commands at end of help message."""
    epilog = command_template
    epilog += "\n\nAvailable Commands:\n"
    for key in available_commands.keys():
        curr_val = available_commands[key]
        curr_desc = f"\n\t- { key }:\n\t\t{ curr_val[0] }\n\t\t{ curr_val[1] }"
        epilog += curr_desc
    return epilog


def parse_commit_for_pr(commit):
    """
    Returns PR ID, for a given commit message (if exists).

    :param commit: A commit message.
    :type commit: str
    """

    pattern = r"\(#(.+?)\)"
    m = re.search(pattern, commit)
    if m:
        pr_id = m.group(1)
        return pr_id


def is_json(json_str):
    """
    Check if string is json-compatible.

    :param json_str: A json-formattable string.
    :type json_str: str
    """
    try:
        json.loads(json_str)
    except ValueError:
        return False
    return True


def validate_args(args):
    """
    Check the arguments formatting and syntax.

    :param args: Contains arguments passed through command line
    :type args: argparse.Namespace
    """

    if args.token and args.username:
        raise ValueError(
            "ERROR:\tOnly one form of authentication is required (either token or user/pass)."
        )

    if not is_json(args.extras):
        raise ValueError(
            f'\n\nERROR:\tParamater "extras" is not formatted correctly. Incorrect syntax:\n\t{ args.extras }'
        )


def parse_message_for_prs(message):
    """
    Returns a list of all occurences of #PR in any message.

    :param message: A blob of text to parse.
    :type message: str
    """
    if not message:
        return []
    pattern = r"#\d+"
    prs = re.findall(pattern, message)
    return prs


######################
#   HELPER COMMANDS  #
######################


def get_issue_close_date(
    issue,
    organization,
    repository,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Returns a string representing the datetime an issue was closed."""
    headers = build_headers(token, username, password)
    issue = issue.lstrip("#")
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/issues/{issue}"
    logging.info(f"URL: { curr_endpoint }")
    response = requests.get(curr_endpoint, headers=headers)
    response.raise_for_status()
    response_dict = json.loads(response.text)
    date = response_dict["closed_at"]
    return date


def get_issues(
    organization,
    repository,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Returns a json object of all issues."""
    headers = build_headers(token, username, password)
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/issues"
    logging.info(f"URL: { curr_endpoint }")
    response = requests.get(curr_endpoint, headers=headers)
    response.raise_for_status()
    response_dict = json.loads(response.text)
    logging.info("Issue Output:")
    logging.info(json.dumps(response_dict, indent=4, sort_keys=True))
    return response_dict


def get_pr_id_from_commit_id(
    organization,
    repository,
    commit_id,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Gets a pull_request id, by parsing commit for (#xxx)."""
    commit_message = get_commit_message(
        organization=organization,
        repository=repository,
        commit_id=commit_id,
        token=token,
        username=username,
        password=password,
    )
    pr_id = parse_commit_for_pr(commit_message)
    logging.info(pr_id)
    return pr_id


def get_target_branch(
    organization,
    repository,
    pull_request_id,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Returns the target branch for a specified pull request."""
    headers = build_headers(token, username, password)
    curr_endpoint = (
        f"{ base_url }/repos/{ organization }/{ repository }/pulls/{ pull_request_id }"
    )
    logging.info(f"Fetching Target Branch for PR #{ pull_request_id }...")
    response = requests.get(
        curr_endpoint,
        headers=headers,
    )
    response.raise_for_status()
    response_dict = json.loads(response.text)
    logging.info(json.dumps(response_dict, sort_keys=True, indent=4))
    target_branch = response_dict["base"]["ref"]
    return target_branch


######################
#      COMMANDS      #
######################


def add_comment(
    organization,
    repository,
    pull_request_id,
    message="automated message via Github API",
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Add a specified comment to a particular pull request."""
    headers = build_headers(token, username, password)
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/issues/{ pull_request_id }/comments"
    if "filename" in kwargs:
        with open(kwargs["filename"], "r") as file:
            message = f"{ message }\n\n\n{ file.read() }"
    logging.info(
        f"Adding comment for PR #{ pull_request_id }...\n\tComment:\n\t'{ message }' "
    )
    print(f"URL: {curr_endpoint}")
    print(f"headers: {headers}")
    print(f"body: {message}")
    response = requests.post(
        curr_endpoint, headers=headers, data=json.dumps({"body": message})
    )
    response.raise_for_status()
    logging.info(response)


def add_labels(
    organization,
    repository,
    pull_request_id,
    labels=["test"],
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Add a set of labels to a particular pull request."""
    headers = build_headers(token, username, password)
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/issues/{ pull_request_id }/labels"
    logging.info(f"URL: { curr_endpoint }")
    logging.info(
        f"Adding labels for PR #{ pull_request_id }...\n\tLabels:\n\t { ', '.join(labels) }"
    )
    response = requests.post(
        curr_endpoint, headers=headers, data=json.dumps({"labels": labels})
    )
    response.raise_for_status()
    logging.info(response)


def close_issue(
    issue,
    organization,
    repository,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Marks a specified issue as closed."""
    headers = build_headers(token, username, password)
    issue = issue.lstrip("#")
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/issues/{issue}"
    logging.info(f"URL: { curr_endpoint }")
    data = {
        "state": "closed",
    }
    response = requests.patch(curr_endpoint, headers=headers, data=json.dumps(data))
    response.raise_for_status()
    response_dict = json.loads(response.text)
    logging.info("Response:")
    logging.info(json.dumps(response_dict, indent=4, sort_keys=True))
    return response_dict


def delete_labels(
    organization,
    repository,
    pull_request_id,
    labels=["test"],
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Deletes a set of specified labels from a particular pull request."""
    headers = build_headers(token, username, password)
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/issues/{ pull_request_id }/labels"
    logging.info(f"URL: { curr_endpoint }")
    logging.info(
        f"Removing labels for PR #{ pull_request_id }...\n\tLabels:\n\t { ', '.join(labels) }"
    )
    for label in labels:
        response = requests.delete(
            f"{curr_endpoint}/{label}",
            headers=headers,
        )
        response.raise_for_status()
        logging.info(f"Label: {label}\n\tResponse: {response}")


def dismiss_single_review(
    organization,
    repository,
    pull_request_id,
    review_id,
    message="automated dismissal via Github API",
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Dismiss a specified review for a particular pull request."""
    headers = build_headers(token, username, password)
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/pulls/{ pull_request_id }/reviews/{ review_id }/dismissals"
    logging.info(f"Dismissing Review '{ review_id }' for PR #{ pull_request_id }")
    response = requests.put(
        curr_endpoint, headers=headers, data=json.dumps({"message": message})
    )
    response.raise_for_status()
    logging.info(response)


def dismiss_all_reviews(
    organization,
    repository,
    pull_request_id,
    message="automated dismissal via Github API",
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Dismiss all reviews for a particular pull request."""
    headers = build_headers(token, username, password)
    logging.info(
        f"Fetching list of reviews for { organization }/{ repository }/{ pull_request_id }."
    )
    response = requests.get(
        f"{ base_url }/repos/{ organization }/{ repository }/pulls/{ pull_request_id }/reviews",
        headers=headers,
    )
    response.raise_for_status()
    body = json.loads(response.text)
    review_ids = []
    for review in body:
        if "id" in review:
            curr_id = review["id"]
            logging.info(f"current ID: { curr_id }")
            review_ids.append(curr_id)

    logging.info(
        f"Dismissing all reviews by ID for { organization }/{ repository }/{ pull_request_id }."
    )
    for review_id in review_ids:
        dismiss_single_review(
            organization, repository, token, pull_request_id, review_id
        )


def get_commit_message(
    organization,
    repository,
    commit_id,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Gets a commit message, using the commit_id."""
    headers = build_headers(token, username, password)
    curr_endpoint = (
        f"{ base_url }/repos/{ organization }/{ repository }/commits/{ commit_id }"
    )
    response = requests.get(
        curr_endpoint,
        headers=headers,
    )
    response.raise_for_status()
    json_response = json.loads(response.text)
    commit_message = json_response["commit"]["message"]
    logging.info(commit_message)
    return commit_message


def get_deploy_issue_number(
    organization,
    repository,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """
    Grabs a list of all open issues.
    Filters for an issue titled "Deploy Request: YYYY-MM-DD".
    Returns an associated issue number.
    """
    issues = get_issues(organization, repository, token, username, password)
    for issue in issues:
        expected_issue_name = "Deploy Request: %s" % pendulum.now().format("YYYY-MM-DD")
        if issue["title"].startswith(expected_issue_name):
            logging.info(
                f"Current issue: {issue['title']} matches {expected_issue_name}."
            )
            return issue["number"]


def get_prs_to_deploy(
    organization,
    repository,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """
    Grabs a list of all open issues.
    Filters for an issue titled "Deploy Request: YYYY-MM-DD".
    Parses the issue's body for all instances of #PR.
    Parses the issue's comments for all instances of #PR.
    Sorts the issues by closed_at datetime (most recent last).
    Returns the list of issues.
    """
    headers = build_headers(token, username, password)
    issues = get_issues(organization, repository, token, username, password)
    pr_list = []
    for issue in issues:
        expected_issue_name = "Deploy Request: %s" % pendulum.now().format("YYYY-MM-DD")
        if issue["title"].startswith(expected_issue_name):
            logging.info(
                f"Current issue: {issue['title']} matches {expected_issue_name}."
            )
            prs_mentioned = parse_message_for_prs(issue["body"])
            pr_list.extend(prs_mentioned)

            comments_url = issue["comments_url"]
            if comments_url:
                comment_response = requests.get(comments_url, headers=headers)
                comment_response.raise_for_status()
                comment_dict = json.loads(comment_response.text)
                logging.info("Comment Output:")
                logging.info(json.dumps(comment_dict, indent=4, sort_keys=True))
                for comment in comment_dict:
                    prs_mentioned = parse_message_for_prs(comment["body"])
                    pr_list.extend(prs_mentioned)
    pr_tup_list_with_dates = [
        (
            pr,
            get_issue_close_date(
                pr, organization, repository, token, username, password
            ),
        )
        for pr in pr_list
    ]
    pr_tup_list_with_dates = sorted(pr_tup_list_with_dates, key=lambda x: x[1])
    logging.info(f"Tuples (PR#, closed_at): {pr_tup_list_with_dates}")
    sorted_pr_list = [x[0].strip("#") for x in pr_tup_list_with_dates]
    return sorted_pr_list


def label_merged_pr(
    organization,
    repository,
    commit_id,
    labels_to_add=["main"],
    labels_to_delete=["in_development"],
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Adds and/or deletes a set of labels for a pull_request (which was merged into develop or main)."""
    commit_message = get_commit_message(
        organization=organization,
        repository=repository,
        commit_id=commit_id,
        token=token,
        username=username,
        password=password,
    )
    pr_id = parse_commit_for_pr(commit_message)
    logging.info(f"PR IDs parsed from commit:\n\t{pr_id}")
    if pr_id:
        add_labels(
            organization=organization,
            repository=repository,
            pull_request_id=pr_id,
            labels=labels_to_add,
            token=token,
            username=username,
            password=password,
        )
        delete_labels(
            organization=organization,
            repository=repository,
            pull_request_id=pr_id,
            labels=labels_to_delete,
            token=token,
            username=username,
            password=password,
        )


def label_prs_mentioned_in_commits(
    organization,
    repository,
    pull_request_id=None,
    commit_id=None,
    labels_to_add=["deployed"],
    labels_to_delete=["undeployed"],
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Adds and/or deletes a set of labels to all PRs mentioned in the commit messages of specified pull_request."""
    if pull_request_id is None:
        pull_request_id = get_pr_id_from_commit_id(
            organization=organization,
            repository=repository,
            commit_id=commit_id,
            token=token,
            username=username,
            password=password,
        )

    if pull_request_id is None:
        # May be a non-standard commit
        return

    commits = list_commits(
        organization=organization,
        repository=repository,
        pull_request_id=pull_request_id,
        token=token,
        username=username,
        password=password,
    )
    dirty_pr_ids = [parse_commit_for_pr(commit) for commit in commits]
    pr_ids = [pr_id for pr_id in dirty_pr_ids if pr_id]
    logging.info(f"PR IDs parsed from commits:\n\t{pr_ids}")

    for pr_id in pr_ids:
        add_labels(
            organization=organization,
            repository=repository,
            pull_request_id=pr_id,
            labels=labels_to_add,
            token=token,
            username=username,
            password=password,
        )
        delete_labels(
            organization=organization,
            repository=repository,
            pull_request_id=pr_id,
            labels=labels_to_delete,
            token=token,
            username=username,
            password=password,
        )


def list_commits(
    organization,
    repository,
    pull_request_id,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Returns a list of all commit messages for a specified pull request."""
    headers = build_headers(token, username, password)
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/pulls/{ pull_request_id }/commits"
    params = {"per_page": 99}
    logging.info(f"Fetching commits for PR #{ pull_request_id }...")
    response = requests.get(curr_endpoint, headers=headers, params=params)
    response.raise_for_status()
    commit_dict = json.loads(response.text)
    commit_messages = []
    for commit in commit_dict:
        commit_messages.append(str(commit["commit"]["message"]))
    logging.info(f"Commit Messages:\n\t{commit_messages}")
    return commit_messages


def list_deleted_files(
    organization,
    repository,
    commit_id,
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Gets a list of files deleted from a commit, using the commit_id."""
    headers = build_headers(token, username, password)
    curr_endpoint = (
        f"{ base_url }/repos/{ organization }/{ repository }/commits/{ commit_id }"
    )
    response = requests.get(
        curr_endpoint,
        headers=headers,
    )
    response.raise_for_status()
    json_response = json.loads(response.text)
    logging.info(json_response)
    files_modified = json_response["files"]
    deleted_files = []
    logging.info("\n\nRaw dump of json response:")
    logging.info(json.dumps(files_modified, indent=4, sort_keys=True))
    logging.info("\n\nList of all files modified:")
    for file in files_modified:
        file_name = file["filename"]
        status = file["status"]
        logging.info(f"Filename: {file_name}. Status: {status}")

    logging.info("\n\nExplanation of what's being deleted:")
    # Only delete files if they have been renamed or removed
    for file in files_modified:
        file_name = file["filename"]
        status = file["status"]
        if status == "removed":
            deleted_files.append(file_name)
            logging.info(f"Deleting {file_name}.")
        if status == "renamed":
            old_file_name = file["previous_filename"]
            logging.info(f"Deleting {old_file_name} (renamed to {file_name})")
            deleted_files.append(old_file_name)
    logging.info("\n\n List of files to be deleted:\n" + "\n".join(deleted_files))
    return deleted_files


def open_pr(
    organization,
    repository,
    head,
    base,
    title,
    body="",
    token=None,
    username=None,
    password=None,
    **kwargs,
):
    """Opens a PR to merge head branch into base branch."""
    headers = build_headers(token, username, password)
    curr_endpoint = f"{ base_url }/repos/{ organization }/{ repository }/pulls"
    logging.info(f"URL: { curr_endpoint }")
    data = {"head": head, "base": base, "title": title, "body": body}
    response = requests.post(curr_endpoint, headers=headers, data=json.dumps(data))
    response.raise_for_status()
    response_dict = json.loads(response.text)
    logging.info("Response:")
    logging.info(json.dumps(response_dict, indent=4, sort_keys=True))
    return response_dict


def main(argv):
    """
    Parses input arguments and formats parameters for generating specified command (API request).

    On error: prints expected syntax, list of commands, and error details.
    """
    parser = argparse.ArgumentParser(
        prog="github_api_call",
        formatter_class=argparse.RawTextHelpFormatter,
        description="A python script that handles GitHub API calls.",
        epilog=format_epilog(),
    )

    parser.add_argument(
        "-o", "--organization", type=str, help="Owner of GitHub repository."
    )
    parser.add_argument(
        "-r", "--repository", type=str, help="Name of the GitHub repository."
    )
    parser.add_argument(
        "-t", "--token", type=str, help="User's GitHub Personal Access Token."
    )
    parser.add_argument(
        "-u", "--username", "--user", type=str, help="User's GitHub username."
    )
    parser.add_argument(
        "-p", "--password", "--pass", type=str, help="User's Github password."
    )
    parser.add_argument(
        "-l",
        "--pull_request_id",
        "--pull-request",
        type=str,
        help="The issue # of the Pull Request.",
    )
    parser.add_argument(
        "-c",
        "--command",
        type=str,
        help="Name of python function associated with API call being made.",
    )
    parser.add_argument(
        "-e", "--extras", type=str, help="Extra dictionary to allow for more arguments."
    )

    args = parser.parse_args()
    validate_args(args)

    parameters = {**vars(args), **json.loads(args.extras)}
    pretty_params = "\n".join(
        [f"{key:<20} {value}" for key, value in parameters.items()]
    )
    logging.info(f"\n\nParsed Parameters:\n{ pretty_params }")
    logging.info("\n\n\n")

    return globals()[args.command](**parameters)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
    print(main(sys.argv[1:]))
