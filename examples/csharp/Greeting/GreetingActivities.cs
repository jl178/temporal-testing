using Temporalio.Activities;

namespace Greeting;

public static class GreetingActivities
{
    [Activity]
    public static string ComposeGreeting(string name) => $"Hello, {name}!";
}
