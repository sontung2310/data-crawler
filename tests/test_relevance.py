from x_influencer_discovery.relevance import hybrid_relevance, lexical_overlap


def test_lexical_overlap_weights_phrases_and_limits_each_term_to_one_hit_per_surface():
    result = lexical_overlap(
        "Marketing automation marketing automation marketing",
        [],
        ["marketing", "marketing automation"],
    )

    assert result["bio_coverage"] == 1.0
    assert result["lexical_overlap"] == 0.45
    assert result["bio_hits"] == ["marketing", "marketing automation"]


def test_lexical_overlap_uses_bio_post_average_and_post_consistency_weights():
    result = lexical_overlap(
        "Marketing automation",
        [{"text": "Marketing"}, {"text": "Unrelated post"}],
        ["marketing", "marketing automation"],
    )

    assert result["bio_coverage"] == 1.0
    assert result["post_coverage"] == 0.125
    assert result["post_consistency"] == 0.5
    assert result["lexical_overlap"] == 0.575


def test_hybrid_relevance_normalizes_cosine_similarity_at_boundaries():
    low = hybrid_relevance("", [], ["marketing"], semantic_similarity=-1.0)
    middle = hybrid_relevance("", [], ["marketing"], semantic_similarity=0.0)
    high = hybrid_relevance("", [], ["marketing"], semantic_similarity=1.0)

    assert low["semantic_similarity"] == 0.0
    assert middle["semantic_similarity"] == 0.5
    assert high["semantic_similarity"] == 1.0
    assert (low["hybrid_relevance"], middle["hybrid_relevance"], high["hybrid_relevance"]) == (0.0, 0.2, 0.4)


def test_candidate_relevance_is_independent_of_other_candidates():
    candidate = hybrid_relevance(
        "Marketing strategist",
        [{"text": "Marketing automation tactics"}],
        ["marketing", "marketing automation"],
        semantic_similarity=0.4,
    )
    _unrelated_batch_member = hybrid_relevance(
        "Photography", [{"text": "Travel"}], ["marketing", "marketing automation"], semantic_similarity=-0.8
    )
    evaluated_again = hybrid_relevance(
        "Marketing strategist",
        [{"text": "Marketing automation tactics"}],
        ["marketing", "marketing automation"],
        semantic_similarity=0.4,
    )

    assert candidate == evaluated_again
