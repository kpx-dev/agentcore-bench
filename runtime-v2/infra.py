"""Create, and later delete, the AWS resources the PoC needs, and nothing else.

Every resource is tagged `agentcore-v2-poc=<run>` and written to the run's state.json the moment it
exists, so teardown removes exactly what this tool made, also after a crash or Ctrl-C.

  ECR repository   private, one per run (mutable tags: the same-tag test moves a tag)
  IAM roles        one execution role per runtime: pull from that repository, write runtime logs
  VPC (optional)   two private subnets in AgentCore-supported AZs, no internet route, an S3 gateway
                   endpoint (ECR layer storage) and interface endpoints for ECR and CloudWatch Logs:
                   the "private subnets + S3 endpoint" topology, with nothing reachable from outside
  usage logs       a log group plus vended-log delivery of each runtime's USAGE_LOGS (per-session,
                   per-second CPU and memory metering, the evidence for the pricing question)
"""

from __future__ import annotations

import ipaddress
import json
import logging
import threading
import time

from botocore.exceptions import ClientError

log = logging.getLogger("poc.infra")

TAG_KEY = "agentcore-v2-poc"
# Availability Zone IDs AgentCore Runtime accepts for VPC subnets, for the Regions that offer V2
# (https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-vpc.html).
SUPPORTED_AZ_IDS = {
    "us-east-1": ["use1-az1", "use1-az2", "use1-az4"],
    "us-east-2": ["use2-az1", "use2-az2", "use2-az3"],
    "us-west-2": ["usw2-az1", "usw2-az2", "usw2-az3"],
    "eu-west-1": ["euw1-az1", "euw1-az2", "euw1-az3"],
    "ap-northeast-1": ["apne1-az1", "apne1-az2", "apne1-az4"],
}
INTERFACE_ENDPOINTS = ("ecr.api", "ecr.dkr", "logs")


def code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


class Infra:
    def __init__(self, session, region: str, account: str, run: str, state: dict, save, lock=None):
        self.region, self.account, self.run, self.state, self.save = region, account, run, state, save
        self.prefix = f"{TAG_KEY}-{run}"
        self._lock = lock or threading.RLock()   # shared with the run: runtimes are created from parallel threads
        self.ecr = session.client("ecr", region_name=region)
        self.iam = session.client("iam")
        self.ec2 = session.client("ec2", region_name=region)
        self.logs = session.client("logs", region_name=region)

    def _tags(self, name: str | None = None) -> list[dict]:
        return [{"Key": TAG_KEY, "Value": self.run}] + ([{"Key": "Name", "Value": name}] if name else [])

    # ------------------------------------------------------------ ECR + IAM
    def ensure_repository(self) -> dict:
        if "ecr_repo" in self.state:
            return self.state["ecr_repo"]
        repo = self.ecr.create_repository(repositoryName=self.prefix, imageTagMutability="MUTABLE",
                                          tags=self._tags())["repository"]
        self.state["ecr_repo"] = {"name": repo["repositoryName"], "arn": repo["repositoryArn"],
                                  "uri": repo["repositoryUri"]}
        self.save()
        log.info("Created ECR repository %s", repo["repositoryUri"])
        return self.state["ecr_repo"]

    def ensure_role(self, key: str, runtime_name: str) -> str:
        """One execution role per runtime: CloudTrail then names the runtime behind every image pull, which
        is how the PoC sees when (and from which tag or digest) AgentCore pulls and builds a snapshot."""
        with self._lock:
            roles = self.state.setdefault("iam_roles", {})
            if key in roles:
                return roles[key]["arn"]
        r, a = self.region, self.account
        trust = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "bedrock-agentcore.amazonaws.com"}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:SourceAccount": a},
                          "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock-agentcore:{r}:{a}:*"}}}]}
        runtime_logs = f"arn:aws:logs:{r}:{a}:log-group:/aws/bedrock-agentcore/runtimes/*"
        policy = {"Version": "2012-10-17", "Statement": [
            {"Sid": "PullPocImage", "Effect": "Allow", "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
             "Resource": self.state["ecr_repo"]["arn"]},
            {"Sid": "EcrToken", "Effect": "Allow", "Action": "ecr:GetAuthorizationToken", "Resource": "*"},
            {"Sid": "RuntimeLogGroups", "Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:DescribeLogStreams"],
             "Resource": runtime_logs},
            {"Sid": "RuntimeLogPolicy", "Effect": "Allow", "Action": "logs:PutResourcePolicy",
             "Resource": f"arn:aws:logs:{r}:{a}:log-group:/aws/bedrock-agentcore/runtimes/{runtime_name}-*"},
            {"Sid": "DescribeLogGroups", "Effect": "Allow", "Action": "logs:DescribeLogGroups",
             "Resource": f"arn:aws:logs:{r}:{a}:log-group:*"},
            {"Sid": "RuntimeLogEvents", "Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
             "Resource": f"{runtime_logs}:log-stream:*"},
            {"Sid": "Traces", "Effect": "Allow", "Resource": "*", "Action": [
                "xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]},
            {"Sid": "Metrics", "Effect": "Allow", "Action": "cloudwatch:PutMetricData", "Resource": "*",
             "Condition": {"StringEquals": {"cloudwatch:namespace": "bedrock-agentcore"}}},
            {"Sid": "WorkloadIdentity", "Effect": "Allow", "Action": [
                "bedrock-agentcore:GetWorkloadAccessToken", "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
                "bedrock-agentcore:GetWorkloadAccessTokenForUserId"],
             "Resource": [f"arn:aws:bedrock-agentcore:{r}:{a}:workload-identity-directory/default",
                          f"arn:aws:bedrock-agentcore:{r}:{a}:workload-identity-directory/default/workload-identity/"
                          f"{runtime_name}*"]},
        ]}
        role = self.iam.create_role(RoleName=f"{self.prefix}-{key}"[:64], AssumeRolePolicyDocument=json.dumps(trust),
                                    Description=f"AgentCore Runtime V2 PoC execution role ({key})", Tags=self._tags())["Role"]
        with self._lock:
            roles[key] = {"name": role["RoleName"], "arn": role["Arn"]}
            self.save()
        self.iam.put_role_policy(RoleName=role["RoleName"], PolicyName="runtime", PolicyDocument=json.dumps(policy))
        log.info("Created execution role %s", role["Arn"])
        return role["Arn"]

    # ------------------------------------------------------------ VPC
    def ensure_vpc(self, cidr: str = "10.88.0.0/16") -> dict:
        """Private-only VPC: no internet gateway, no NAT; AWS services through VPC endpoints."""
        vpc_state = self.state.setdefault("vpc", {})
        if vpc_state.get("ready"):
            return vpc_state
        ec2, name = self.ec2, self.prefix
        az_ids = SUPPORTED_AZ_IDS.get(self.region)
        if not az_ids:
            raise RuntimeError(f"No AgentCore VPC AZ list for {self.region}; pass --subnets/--security-groups")
        zones = {z["ZoneId"] for z in ec2.describe_availability_zones(
            Filters=[{"Name": "state", "Values": ["available"]}])["AvailabilityZones"]}
        az_ids = [z for z in az_ids if z in zones][:2]

        if "id" not in vpc_state:
            vpc = ec2.create_vpc(CidrBlock=cidr, TagSpecifications=[{"ResourceType": "vpc", "Tags": self._tags(name)}])
            vpc_state["id"] = vpc["Vpc"]["VpcId"]
            self.save()
            ec2.get_waiter("vpc_available").wait(VpcIds=[vpc_state["id"]])
            ec2.modify_vpc_attribute(VpcId=vpc_state["id"], EnableDnsSupport={"Value": True})
            ec2.modify_vpc_attribute(VpcId=vpc_state["id"], EnableDnsHostnames={"Value": True})
            log.info("Created VPC %s (%s)", vpc_state["id"], cidr)
        vpc_id = vpc_state["id"]
        nets = list(ipaddress.ip_network(cidr).subnets(new_prefix=24))
        subnets = vpc_state.setdefault("subnets", [])
        for i, az_id in enumerate(az_ids[len(subnets):], start=len(subnets)):
            sub = ec2.create_subnet(VpcId=vpc_id, CidrBlock=str(nets[i]), AvailabilityZoneId=az_id,
                                    TagSpecifications=[{"ResourceType": "subnet", "Tags": self._tags(f"{name}-{az_id}")}])
            subnets.append(sub["Subnet"]["SubnetId"])
            self.save()
            log.info("Created private subnet %s in %s", subnets[-1], az_id)
        if "route_table" not in vpc_state:
            rt = ec2.create_route_table(VpcId=vpc_id, TagSpecifications=[
                {"ResourceType": "route-table", "Tags": self._tags(f"{name}-private")}])
            vpc_state["route_table"] = rt["RouteTable"]["RouteTableId"]
            vpc_state["associations"] = []
            self.save()
            for sub in subnets:
                assoc = ec2.associate_route_table(RouteTableId=vpc_state["route_table"], SubnetId=sub)
                vpc_state["associations"].append(assoc["AssociationId"])
                self.save()
        if "sg_runtime" not in vpc_state:
            vpc_state["sg_runtime"] = ec2.create_security_group(
                GroupName=f"{name}-runtime", VpcId=vpc_id,
                Description="AgentCore V2 PoC runtime ENIs: no inbound, outbound only",
                TagSpecifications=[{"ResourceType": "security-group", "Tags": self._tags(f"{name}-runtime")}])["GroupId"]
            self.save()
        if "sg_endpoints" not in vpc_state:
            vpc_state["sg_endpoints"] = ec2.create_security_group(
                GroupName=f"{name}-endpoints", VpcId=vpc_id,
                Description="AgentCore V2 PoC interface endpoints: HTTPS from the runtime SG only",
                TagSpecifications=[{"ResourceType": "security-group", "Tags": self._tags(f"{name}-endpoints")}])["GroupId"]
            self.save()
            ec2.authorize_security_group_ingress(GroupId=vpc_state["sg_endpoints"], IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
                "UserIdGroupPairs": [{"GroupId": vpc_state["sg_runtime"], "Description": "PoC runtime"}]}])
        endpoints = vpc_state.setdefault("endpoints", {})
        if "s3" not in endpoints:
            # Least-privilege policy from the AgentCore VPC guide: ECR's layer bucket only.
            policy = {"Statement": [{"Sid": "AllowECRLayerAccess", "Principal": "*", "Action": ["s3:GetObject"],
                                     "Effect": "Allow", "Resource": [f"arn:aws:s3:::prod-{self.region}-starport-layer-bucket/*"]}]}
            ep = ec2.create_vpc_endpoint(VpcEndpointType="Gateway", VpcId=vpc_id, ServiceName=f"com.amazonaws.{self.region}.s3",
                                         RouteTableIds=[vpc_state["route_table"]], PolicyDocument=json.dumps(policy),
                                         TagSpecifications=[{"ResourceType": "vpc-endpoint", "Tags": self._tags(f"{name}-s3")}])
            endpoints["s3"] = ep["VpcEndpoint"]["VpcEndpointId"]
            self.save()
        for svc in INTERFACE_ENDPOINTS:
            if svc in endpoints:
                continue
            ep = ec2.create_vpc_endpoint(VpcEndpointType="Interface", VpcId=vpc_id,
                                         ServiceName=f"com.amazonaws.{self.region}.{svc}", SubnetIds=subnets,
                                         SecurityGroupIds=[vpc_state["sg_endpoints"]], PrivateDnsEnabled=True,
                                         TagSpecifications=[{"ResourceType": "vpc-endpoint", "Tags": self._tags(f"{name}-{svc}")}])
            endpoints[svc] = ep["VpcEndpoint"]["VpcEndpointId"]
            self.save()
        self._wait_endpoints(list(endpoints.values()))
        vpc_state["ready"] = True
        vpc_state["az_ids"] = az_ids
        self.save()
        log.info("VPC ready: subnets %s, runtime SG %s, endpoints %s", subnets, vpc_state["sg_runtime"], endpoints)
        return vpc_state

    def _wait_endpoints(self, ids: list[str], timeout_s: int = 900) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            states = {e["VpcEndpointId"]: e["State"].lower()
                      for e in self.ec2.describe_vpc_endpoints(VpcEndpointIds=ids)["VpcEndpoints"]}
            if all(s == "available" for s in states.values()):
                return
            if any(s in ("failed", "rejected") for s in states.values()):
                raise RuntimeError(f"VPC endpoint creation failed: {states}")
            time.sleep(10)
        raise TimeoutError(f"VPC endpoints not available after {timeout_s}s")

    # ------------------------------------------------------------ USAGE_LOGS
    def ensure_usage_destination(self) -> dict:
        """The log group and delivery destination every runtime's USAGE_LOGS go to. Call it once, before the
        runtimes are created in parallel: concurrent PutDeliveryDestination calls conflict."""
        with self._lock:
            usage = self.state.setdefault("usage", {"sources": [], "deliveries": []})
            if "destination_arn" in usage:
                return usage
            group = f"/aws/vendedlogs/bedrock-agentcore/runtime/USAGE_LOGS/{self.prefix}"
            try:
                self.logs.create_log_group(logGroupName=group, tags={TAG_KEY: self.run})
            except self.logs.exceptions.ResourceAlreadyExistsException:
                pass
            usage["log_group"] = group
            self.save()
            self.logs.put_retention_policy(logGroupName=group, retentionInDays=30)
            kwargs = {"name": f"{self.prefix}-usage"[:60], "deliveryDestinationType": "CWL", "outputFormat": "json",
                      "deliveryDestinationConfiguration": {
                          "destinationResourceArn": f"arn:aws:logs:{self.region}:{self.account}:log-group:{group}"}}
            try:
                dest = self.logs.put_delivery_destination(**kwargs, tags={TAG_KEY: self.run})["deliveryDestination"]
            except self.logs.exceptions.ConflictException:
                dest = self.logs.put_delivery_destination(**kwargs)["deliveryDestination"]  # exists: tags only on create
            usage.update(destination_name=dest["name"], destination_arn=dest["arn"])
            self.save()
            return usage

    def add_usage_source(self, runtime_name: str, runtime_arn: str) -> None:
        name = f"{runtime_name}-usage"[:60]
        # Serialized: each CreateDelivery rewrites the destination log group's resource policy, and concurrent calls
        # fail with ConflictException or "Failed to set permission".
        with self._lock:
            try:
                usage = self.ensure_usage_destination()
                self.logs.put_delivery_source(name=name, logType="USAGE_LOGS", resourceArn=runtime_arn,
                                              tags={TAG_KEY: self.run})
                usage["sources"].append(name)
                self.save()
                for attempt in range(1, 6):
                    try:
                        delivery = self.logs.create_delivery(deliverySourceName=name, tags={TAG_KEY: self.run},
                                                             deliveryDestinationArn=usage["destination_arn"])["delivery"]
                        break
                    except ClientError as exc:
                        if code(exc) in ("ConflictException", "AccessDeniedException", "ThrottlingException") and attempt < 5:
                            time.sleep(3 * attempt)
                            continue
                        raise
                usage["deliveries"].append(delivery["id"])
                self.save()
            except ClientError as exc:
                # The pricing evidence degrades to CloudWatch metrics; every other question is unaffected.
                log.warning("USAGE_LOGS delivery for %s not enabled: %s", runtime_name, exc)

    # ------------------------------------------------------------ teardown
    def teardown(self, delete_logs: bool = True) -> list[str]:
        """Delete what state.json lists, in dependency order. Returns what could not be deleted yet."""
        left: list[str] = []
        usage = self.state.get("usage", {})
        # USAGE_LOGS reach the log group 10-25 min late: keeping the logs means keeping the delivery wiring too.
        for did in list(usage.get("deliveries", []) if delete_logs else []):
            self._try(self.logs.delete_delivery, {"id": did}, f"delivery {did}", left, ok=("ResourceNotFoundException",))
            usage["deliveries"].remove(did)
        for src in list(usage.get("sources", []) if delete_logs else []):
            self._try(self.logs.delete_delivery_source, {"name": src}, f"delivery source {src}", left,
                      ok=("ResourceNotFoundException",))
            usage["sources"].remove(src)
        if usage.get("destination_name") and delete_logs:
            self._try(self.logs.delete_delivery_destination, {"name": usage["destination_name"]},
                      "delivery destination", left, ok=("ResourceNotFoundException",))
            usage.pop("destination_name")
            usage.pop("destination_arn", None)
        groups = ([usage["log_group"]] if usage.get("log_group") else []) + self.state.get("runtime_log_groups", [])
        if delete_logs:
            for group in groups:
                self._try(self.logs.delete_log_group, {"logGroupName": group}, f"log group {group}", left,
                          ok=("ResourceNotFoundException",))
        self.save()

        vpc = self.state.get("vpc", {})
        eps = list(vpc.get("endpoints", {}).values())
        if eps:
            # Interface endpoints are the only resources here that bill by the hour: delete them first.
            self._try(self.ec2.delete_vpc_endpoints, {"VpcEndpointIds": eps}, "VPC endpoints", left,
                      ok=("InvalidVpcEndpointId.NotFound",))
            self._wait_gone_endpoints(eps)
            vpc["endpoints"] = {}
        for key in ("sg_endpoints", "sg_runtime"):
            if vpc.get(key) and self._try(self.ec2.delete_security_group, {"GroupId": vpc[key]}, f"security group {vpc[key]}",
                                          left, ok=("InvalidGroup.NotFound",)):
                vpc.pop(key)
        for assoc in list(vpc.get("associations", [])):
            if self._try(self.ec2.disassociate_route_table, {"AssociationId": assoc}, f"route association {assoc}", left,
                         ok=("InvalidAssociationID.NotFound",)):
                vpc["associations"].remove(assoc)
        if vpc.get("route_table") and not vpc.get("associations"):
            if self._try(self.ec2.delete_route_table, {"RouteTableId": vpc["route_table"]}, "route table", left,
                         ok=("InvalidRouteTableID.NotFound",)):
                vpc.pop("route_table")
        for sub in list(vpc.get("subnets", [])):
            if self._try(self.ec2.delete_subnet, {"SubnetId": sub}, f"subnet {sub}", left, ok=("InvalidSubnetID.NotFound",)):
                vpc["subnets"].remove(sub)
        if vpc.get("id") and not vpc.get("subnets"):
            if self._try(self.ec2.delete_vpc, {"VpcId": vpc["id"]}, f"VPC {vpc['id']}", left, ok=("InvalidVpcID.NotFound",)):
                self.state.pop("vpc")
        self.save()

        roles = self.state.get("iam_roles", {})
        for key, role in list(roles.items()):
            self._try(self.iam.delete_role_policy, {"RoleName": role["name"], "PolicyName": "runtime"}, "role policy", left,
                      ok=("NoSuchEntity",))
            if self._try(self.iam.delete_role, {"RoleName": role["name"]}, f"IAM role {role['name']}", left,
                         ok=("NoSuchEntity",)):
                roles.pop(key)
        repo = self.state.get("ecr_repo")
        if repo and self._try(self.ecr.delete_repository, {"repositoryName": repo["name"], "force": True},
                              f"ECR repository {repo['name']}", left, ok=("RepositoryNotFoundException",)):
            self.state.pop("ecr_repo")
        self.save()
        return left

    def _wait_gone_endpoints(self, ids: list[str], timeout_s: int = 600) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                live = [e for e in self.ec2.describe_vpc_endpoints(VpcEndpointIds=ids)["VpcEndpoints"]
                        if e["State"].lower() != "deleted"]
            except ClientError as exc:
                if code(exc) == "InvalidVpcEndpointId.NotFound":
                    return
                raise
            if not live:
                return
            time.sleep(10)

    @staticmethod
    def _try(call, kwargs: dict, what: str, left: list[str], ok: tuple[str, ...] = ()) -> bool:
        try:
            call(**kwargs)
            log.info("Deleted %s", what)
            return True
        except ClientError as exc:
            if code(exc) in ok:
                return True
            # AgentCore keeps its VPC network interfaces for up to 8 hours after a runtime is deleted,
            # so subnets and the runtime security group can stay in use for a while.
            log.warning("Could not delete %s yet: %s", what, exc.response["Error"].get("Message", code(exc)))
            left.append(what)
            return False
