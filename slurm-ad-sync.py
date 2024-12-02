#!/usr/bin/env python3

import re
import subprocess
import argparse
from samba.auth import system_session
from samba.credentials import Credentials
from samba.param import LoadParm
from samba.samdb import SamDB

# Configuration
SERVICE_ACCOUNT = "slurm-sync"
PASSWORD = "-pass4Slurm-"
DOMAIN = "grit.ucsb.edu"
SERVER = "dc1.grit.ucsb.edu"
BASE_DN = "ou=GRIT Users,dc=grit,dc=ucsb,dc=edu"

def connect_to_ad(service_account, password, domain, server):
    """Connect to AD using Samba Python bindings."""
    try:
        lp = LoadParm()
        lp.load_default()
        creds = Credentials()
        creds.guess(lp)
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
        groups = samdb.search(
            base=BASE_DN,  # Use BASE_DN from the configuration
            scope=2,
            expression=query,
            attrs=["cn", "member", "sAMAccountName"]
        )
        return groups
    except Exception as e:
        print(f"Error querying AD groups: {e}")
        return []

def slurm_group_exists(group_name):
    """Check if a group already exists in Slurm."""
    try:
        result = subprocess.run(
            ["sacctmgr", "list", "account", "format=Account,Cluster", "--parsable2"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        output = result.stdout
        print(f"Checking existence of group '{group_name}'. Command output:\n{output}")
        # Check if the group exists in any cluster
        for line in output.splitlines():
            account = line.split('|')[0]  # Extract the Account column
            if account == group_name:
                print(f"Group '{group_name}' found in line: {line}")
                return True
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error checking Slurm group: {e}")
        return False

def slurm_user_exists(username):
    """Check if a user already exists in Slurm."""
    try:
        result = subprocess.run(
            ["sacctmgr", "list", "user", username, "format=User"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        output = result.stdout.decode('utf-8')
        return username in output
    except subprocess.CalledProcessError as e:
        print(f"Error checking Slurm user: {e}")
        return False

def slurm_user_in_group(username, group_name):
    """Check if a user's DefaultAccount is already set to the specified group."""
    try:
        result = subprocess.run(
            ["sacctmgr", "list", "user", username, "format=DefaultAccount", "--parsable2"],
            stdout=subprocess.PIPE,
            text=True
        )
        output = result.stdout
        print(f"Checking if user '{username}' is in group '{group_name}'. Command output:\n{output}")
        # Parse the output to check if DefaultAccount matches the group_name
        for line in output.splitlines():
            if group_name == line.strip():
                return True
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error checking Slurm user-group association: {e}")
        return False

def add_to_slurmdbd(group_name, members, dry_run, samdb):
    """Add groups and users to Slurm database."""
    try:
        # Ensure group_name is a string
        if isinstance(group_name, bytes):
            group_name = group_name.decode('utf-8')

        # Check if the group already exists
        if not slurm_group_exists(group_name):
            if dry_run:
                print(f"[DRY RUN] Would add group {group_name} to slurmdbd.")
            else:
                subprocess.run(["sacctmgr", "-i", "add", "account", group_name], check=True)
                print(f"Added group {group_name} to slurmdbd.")
        else:
            print(f"Group '{group_name}' already exists in slurmdbd, skipping addition.")

        # Add users to the group
        for member in members:
            # Query the sAMAccountName for each member DN
            user_entry = samdb.search(base=member, scope=0, attrs=["sAMAccountName"])
            username = extract_username(user_entry[0]) if user_entry else None

            if username:
                # Ensure username is a string
                if isinstance(username, bytes):
                    username = username.decode('utf-8')

                if not slurm_user_exists(username):
                    if dry_run:
                        print(f"[DRY RUN] Would add user {username} to group {group_name}.")
                    else:
                        subprocess.run(["sacctmgr", "-i", "add", "user", username, "account=" + group_name], check=True)
                        print(f"Added user {username} to group {group_name}.")
                else:
                    if not slurm_user_in_group(username, group_name):
                        if dry_run:
                            print(f"[DRY RUN] Would associate user {username} with group {group_name}.")
                        else:
                            subprocess.run(
                                ["sacctmgr", "-i", "modify", "user", "where", f"name={username}", "set", f"DefaultAccount={group_name}"],
                                check=True
                            )
                            print(f"Associated user {username} with group {group_name}.")
                    else:
                        print(f"User {username} is already associated with group {group_name}.")
    except subprocess.CalledProcessError as e:
        print(f"Error updating slurmdbd: {e}")

def extract_username(ad_entry):
    """Extract sAMAccountName from an AD entry."""
    try:
        # Ensure we have the sAMAccountName attribute
        if "sAMAccountName" in ad_entry:
            username = ad_entry["sAMAccountName"][0]
            return username
        else:
            print(f"No sAMAccountName found for entry: {ad_entry}")
            return None
    except Exception as e:
        print(f"Error extracting sAMAccountName: {e}")
        return None

def main():
    """Main function to synchronize AD groups and users with Slurm."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Synchronize AD groups and users with Slurm.")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode to preview changes.")
    args = parser.parse_args()

    dry_run = args.dry_run

    # Connect to AD
    samdb = connect_to_ad(SERVICE_ACCOUNT, PASSWORD, DOMAIN, SERVER)
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
        add_to_slurmdbd(group_name, members, dry_run, samdb)  # Pass samdb here

if __name__ == "__main__":
    main()
