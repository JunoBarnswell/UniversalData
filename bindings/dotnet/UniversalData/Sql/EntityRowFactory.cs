using System.Reflection;

namespace UniversalData;

internal sealed class EntityRowFactory<T>
{
    private readonly ConstructorBinding[] constructors;
    private readonly bool parameterless;
    internal IReadOnlyList<(EntityMember Member, uint Ordinal)> Projection { get; }
    internal object?[] Values { get; }

    internal EntityRowFactory(IReadOnlyList<(EntityMember Member, uint Ordinal)> projection)
    {
        Projection = projection;
        Values = new object?[projection.Count];
        var type = typeof(T);
        parameterless = type.IsValueType || type.GetConstructor(Type.EmptyTypes) is not null;
        constructors = parameterless ? [] : BindConstructors(type);
    }

    internal T Materialize()
    {
        if (parameterless)
        {
            object entity = Activator.CreateInstance(typeof(T))!;
            for (int index = 0; index < Projection.Count; index++) Projection[index].Member.Set(entity, Values[index]);
            return (T)entity;
        }
        foreach (var binding in constructors)
        {
            var usable = true;
            for (int index = 0; index < binding.Ordinals.Length; index++)
            {
                int ordinal = binding.Ordinals[index];
                if (ordinal < 0) continue;
                object? value = Values[ordinal];
                var parameter = binding.Parameters[index];
                if (value is null && parameter.ParameterType.IsValueType &&
                    Nullable.GetUnderlyingType(parameter.ParameterType) is null)
                {
                    usable = false;
                    break;
                }
                binding.Arguments[index] = value is null ? null :
                    SqlValueCodec.ConvertTo(value, parameter.ParameterType, Projection[ordinal].Member.ColumnName);
            }
            if (!usable) continue;
            object entity;
            try { entity = binding.Constructor.Invoke(binding.Arguments); }
            catch (TargetInvocationException error)
            { throw new InvalidOperationException($"Could not construct entity '{typeof(T).FullName}'.", error.InnerException ?? error); }
            for (int index = 0; index < Projection.Count; index++)
                if (!binding.Consumed[index]) Projection[index].Member.Set(entity, Values[index]);
            return (T)entity;
        }
        throw new InvalidOperationException($"Entity '{typeof(T).FullName}' needs a public parameterless constructor or a constructor whose parameters match its mapped columns.");
    }

    private ConstructorBinding[] BindConstructors(Type type)
    {
        var byName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int index = 0; index < Projection.Count; index++) byName.Add(Projection[index].Member.Property.Name, index);
        for (int index = 0; index < Projection.Count; index++) byName.TryAdd(Projection[index].Member.ColumnName, index);
        var bindings = new List<ConstructorBinding>();
        foreach (var constructor in type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                     .OrderByDescending(value => value.GetParameters().Length))
        {
            var parameters = constructor.GetParameters();
            var arguments = new object?[parameters.Length];
            var ordinals = new int[parameters.Length];
            var consumed = new bool[Projection.Count];
            bool usable = true;
            for (int index = 0; index < parameters.Length; index++)
            {
                var parameter = parameters[index];
                if (byName.TryGetValue(parameter.Name ?? "", out int ordinal))
                {
                    ordinals[index] = ordinal;
                    consumed[ordinal] = true;
                }
                else if (parameter.HasDefaultValue)
                {
                    ordinals[index] = -1;
                    arguments[index] = parameter.DefaultValue;
                }
                else
                {
                    usable = false;
                    break;
                }
            }
            if (usable) bindings.Add(new(constructor, parameters, arguments, ordinals, consumed));
        }
        return bindings.ToArray();
    }

    private sealed record ConstructorBinding(ConstructorInfo Constructor, ParameterInfo[] Parameters,
        object?[] Arguments, int[] Ordinals, bool[] Consumed);
}
