import re
import subprocess
from samba.auth import system_session
from samba.credentials import Credentials
from samba.param import LoadParm
from samba.samdb import SamDB


def connect_to_ad(service_account, password, domain, server):
    """Connect to AD using Samba Python bindings."""
    try:
        lp = LoadParm()
        lp.load_default()
        creds = Credentials()
        creds.set_username(service_account)
        creds.set_password(password)
        creds.set_domain(domain)
        samdb = SamDB(url=f"ldap://{server}", session_info=system_session(), credentials=creds, lp=lp)
        return samdb
    except Exception as e:
        print(f"Error connecting to AD: {e}")
        return None


def get_slurm_groups(samdb):
    """Retrieve AD groups starting with 'slurm_'."""
    try:
        query = "(cn=slurm_*)"
        groups = samdb.search(base="", scope=2, expression=query, attrs=["cn", "member"])
        return groups
    except Exception as e:
        print(f"Error querying AD groups: {e}")
        return []


def slurm_group_exists(group_name):
    """Check if a group already exists in Slurm."""
    try:
        result = subprocess.run(
            ["sacctmgr", "list", "account", group_name, "format=Account"],
            stdout=subprocess.PIPE,
            text=True
        )
        return group_name in result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error checking Slurm group: {e}")
        return False


def slurm_user_exists(username):
    """Check if a user already exists in Slurm."""
    try:
        result = subprocess.run(
            ["sacctmgr", "list", "user", username, "format=User"],
            stdout=subprocess.PIPE,
            text=True
        )
        return username in result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error checking Slurm user: {e}")
        return False


def slurm_user_in_group(username, group_name):
    """Check if a user is already associated with a Slurm group."""
    try:
        result = subprocess.run(
            ["sacctmgr", "list", "user", username, "format=Account"],
            stdout=subprocess.PIPE,
            text=True
        )
        return group_name in result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error checking Slurm user-group association: {e}")
        return False


def add_to_slurmdbd(group_name, members):
    """Add groups and users to Slurm database."""
    try:
        # Check if the group already exists
        if not slurm_group_exists(group_name):
            subprocess.run(["sacctmgr", "-i", "add", "account", group_name], check=True)
            print(f"Added group {group_name} to slurmdbd.")
        else:
            print(f"Group {group_name} already exists in slurmdbd.")

        # Add users to the group
        for member in members:
            username = extract_username(member)
            if username:
                if not slurm_user_exists(username):
                    # Add the user to Slurm if they do not exist
                    subprocess.run(["sacctmgr", "-i", "add", "user", username, "account=" + group_name], check=True)
                    print(f"Added user {username} to group {group_name}.")
                else:
                    # Check if the user is already in the group
                    if not slurm_user_in_group(username, group_name):
                        subprocess.run(
                            ["sacctmgr", "-i", "modify", "user", username, "set", "account=" + group_name],
                            check=True
                        )
                        print(f"Associated user {username} with group {group_name}.")
                    else:
                        print(f"User {username} is already associated with group {group_name}.")
    except subprocess.CalledProcessError as e:
        print(f"Error updating slurmdbd: {e}")


def extract_username(ad_dn):
    """Extract username from AD DN (distinguished name)."""
    match = re.search(r"CN=([^,]+)", ad_dn)
    if match:
        return match.group(1)
    return None


def main():
    """
    Main function to synchronize AD groups and users with Slurm.
    This script should be scheduled to run periodically.
    """
    # Configuration
    service_account = "your_service_account"
    password = "your_service_account_password"
    domain = "your.domain.com"
    server = "your.ad.server"

    # Connect to AD
    samdb = connect_to_ad(service_account, password, domain, server)
    if not samdb:
        print("Failed to connect to AD. Exiting.")
        return

    # Get slurm groups
    groups = get_slurm_groups(samdb)
    if not groups:
        print("No slurm groups found in AD.")
        return

    # Process each group
    for group in groups:
        group_name = group.get("cn")[0]
        members = group.get("member", [])  # Get members, or an empty list if none
        add_to_slurmdbd(group_name, members)


if __name__ == "__main__":
    main()
