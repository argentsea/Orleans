using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Reminders;

namespace ArgentSea.Orleans;

/// <summary>
/// Validator for <see cref="ReminderOptions"/>.
/// </summary>
public sealed class ArgentSeaReminderOptionsValidator : IConfigurationValidator
{
    private readonly ILogger<ArgentSeaReminderOptionsValidator> logger;
    private readonly IOptions<ReminderOptions> options;

    /// <summary>
    /// Initializes a new instance of the <see cref="ArgentSeaReminderOptionsValidator"/> class.
    /// </summary>
    /// <param name="logger">
    /// The logger.
    /// </param>
    /// <param name="reminderOptions">
    /// The reminder options.
    /// </param>
    public ArgentSeaReminderOptionsValidator(ILogger<ArgentSeaReminderOptionsValidator> logger, IOptions<ReminderOptions> reminderOptions)
    {
        this.logger = logger;
        options = reminderOptions;
    }

    /// <inheritdoc />
    public void ValidateConfiguration()
    {
        if (options.Value.MinimumReminderPeriod < TimeSpan.Zero)
        {
            throw new OrleansConfigurationException($"{nameof(ReminderOptions)}.{nameof(ReminderOptions.MinimumReminderPeriod)} must not be less than {TimeSpan.Zero}");
        }

        if (options.Value.MinimumReminderPeriod.TotalMinutes < ReminderOptionsDefaults.MinimumReminderPeriodMinutes)
        {
            logger.LogWarning((int)RSErrorCode.RS_FastReminderInterval,
                    $"{nameof(ReminderOptions)}.{nameof(ReminderOptions.MinimumReminderPeriod)} is {options.Value.MinimumReminderPeriod:g} (default {ReminderOptionsDefaults.MinimumReminderPeriodMinutes:g}. High-Frequency reminders are unsuitable for production use.");
        }
    }
}
internal static class ReminderOptionsDefaults
{
    /// <summary>
    /// Minimum period for registering a reminder ... we want to enforce a lower bound <see cref="ReminderOptions.MinimumReminderPeriod"/>.
    /// </summary>
    /// <remarks>Increase this period, reminders are supposed to be less frequent ... we use 2 seconds just to reduce the running time of the unit tests</remarks>
    public const uint MinimumReminderPeriodMinutes = 1;

    /// <summary>
    /// Period (in minutes) between refreshing local reminder list to reflect the global reminder table every <see cref="ReminderOptions.RefreshReminderListPeriod"/>.
    /// </summary>
    public const uint RefreshReminderListPeriodMinutes = 5;

    /// <summary>
    /// The maximum amount of time (in minutes) to attempt to initialize reminders giving up <see cref="ReminderOptions.InitializationTimeout"/>.
    /// </summary>
    public const uint InitializationTimeoutMinutes = 5;
}