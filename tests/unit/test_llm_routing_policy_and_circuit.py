from app.services.llm.reliability import CircuitBreaker, CircuitBreakerConfig
from app.services.llm.routing import FallbackPolicy, RoutingPolicyContext


def test_fallback_policy_matrix():
    policy = FallbackPolicy(
        policy_version="test-v1",
        restricted_classes={"restricted", "high_risk"},
        enabled=True,
    )

    cases = [
        ("timeout", True, False, "standard", True),
        ("network", False, True, "standard", True),
        ("hub_5xx", True, False, "restricted", False),
        ("none", True, True, "standard", False),
        ("timeout", False, False, "standard", False),
    ]

    for outage_reason, cannot_wait, sla_critical, policy_class, expected_allowed in cases:
        decision = policy.evaluate(
            context=RoutingPolicyContext(
                cannot_wait=cannot_wait,
                sla_critical=sla_critical,
                policy_class=policy_class,
            ),
            outage_reason=outage_reason,
        )
        assert decision.allowed is expected_allowed


def test_circuit_breaker_transitions_closed_open_half_open_closed():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            window_seconds=30,
            min_requests=4,
            failure_ratio_threshold=0.5,
            open_duration_seconds=10,
            half_open_max_requests=1,
        )
    )
    ts = 1000.0

    breaker.record_failure(now=ts + 1)
    breaker.record_failure(now=ts + 2)
    breaker.record_failure(now=ts + 3)
    breaker.record_failure(now=ts + 4)

    assert breaker.state == "open"
    allowed, reason = breaker.allow_request(now=ts + 5)
    assert allowed is False
    assert reason == "circuit_open"

    allowed, reason = breaker.allow_request(now=ts + 15)
    assert allowed is True
    assert reason is None
    assert breaker.state == "half_open"

    breaker.record_success(now=ts + 15.1)
    assert breaker.state == "closed"


def test_circuit_breaker_reopens_on_half_open_failure():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            window_seconds=30,
            min_requests=2,
            failure_ratio_threshold=0.5,
            open_duration_seconds=3,
            half_open_max_requests=1,
        )
    )
    ts = 2000.0

    breaker.record_failure(now=ts + 1)
    breaker.record_failure(now=ts + 2)
    assert breaker.state == "open"

    allowed, _ = breaker.allow_request(now=ts + 6)
    assert allowed is True
    assert breaker.state == "half_open"

    breaker.record_failure(now=ts + 6.1)
    assert breaker.state == "open"
