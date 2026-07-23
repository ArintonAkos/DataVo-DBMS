using System.Globalization;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

internal sealed class DataVoArenaMutationState
{
    private readonly Dictionary<int, int> _scores;

    public DataVoArenaMutationState(ArenaLiveViewsProfile profile)
    {
        ArenaLiveViewsPlayer[] players = ArenaLiveViewsFixture.CreateInitialPlayers(profile);
        _scores = new Dictionary<int, int>(players.Length);
        foreach (ArenaLiveViewsPlayer player in players)
        {
            _scores.Add(player.Id, player.Score);
        }
    }

    public int GetUpdatedScore(ArenaLiveViewsMutation mutation)
    {
        if (!_scores.TryGetValue(mutation.PlayerId, out int currentScore))
        {
            throw new InvalidOperationException(
                "Arena mutation score state does not contain player " +
                mutation.PlayerId.ToString(CultureInfo.InvariantCulture) + ".");
        }

        return checked(currentScore + mutation.ScoreDelta);
    }

    public void SetScore(ArenaLiveViewsMutation mutation, int updatedScore) =>
        _scores[mutation.PlayerId] = updatedScore;
}
