using System;
using System.Globalization;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Application = Microsoft.SqlServer.Dts.Runtime.Application;
using DataType = Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType;
using Package = Microsoft.SqlServer.Dts.Runtime.Package;
using TaskHost = Microsoft.SqlServer.Dts.Runtime.TaskHost;
using ConnectionManager = Microsoft.SqlServer.Dts.Runtime.ConnectionManager;

public static class GoldenRows
{
    private static ConnectionManager FlatFile(Package package, string name, string path,
        string[] names, DataType[] types, int[] lengths)
    {
        var connection = package.Connections.Add("FLATFILE");
        connection.Name = name;
        connection.ConnectionString = path;
        var file = (IDTSConnectionManagerFlatFile100)connection.InnerObject;
        file.Format = "Delimited";
        file.LocaleID = 1033;
        file.CodePage = 1252;
        file.HeaderRowDelimiter = "\r\n";
        for (var i = 0; i < names.Length; i++)
        {
            var column = file.Columns.Add();
            column.ColumnType = "Delimited";
            column.ColumnDelimiter = i + 1 == names.Length ? "\r\n" : ",";
            column.ColumnWidth = lengths[i];
            column.MaximumWidth = lengths[i];
            column.DataType = types[i];
            ((IDTSName100)column).Name = names[i];
        }
        return connection;
    }

    private static IDTSComponentMetaData100 AddComponent(MainPipe pipe, Application app, string name)
    {
        var component = pipe.ComponentMetaDataCollection.New();
        component.ComponentClassID = app.PipelineComponentInfos[name].CreationName;
        component.Name = name;
        component.Instantiate().ProvideComponentProperties();
        return component;
    }

    private static void Attach(MainPipe pipe, IDTSOutput100 output, IDTSInput100 input)
    {
        pipe.PathCollection.New().AttachPathAndPropagateNotifications(output, input);
    }

    private static void Connect(IDTSComponentMetaData100 component, ConnectionManager connection)
    {
        component.RuntimeConnectionCollection[0].ConnectionManagerID = connection.ID;
        component.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(connection);
        var design = component.Instantiate();
        design.AcquireConnections(null);
        try
        {
            design.ReinitializeMetaData();
        }
        finally
        {
            design.ReleaseConnections();
        }
    }

    private static IDTSOutputColumn100 Derived(IDTSComponentMetaData100 component, string name,
        DataType type, int length, string friendly, string expression)
    {
        var output = component.OutputCollection[0];
        var design = component.Instantiate();
        var result = design.InsertOutputColumnAt(output.ID, output.OutputColumnCollection.Count, name, "");
        result.SetDataTypeProperties(type, length, 0, 0, 0);
        design.SetOutputColumnProperty(output.ID, result.ID, "FriendlyExpression", friendly);
        design.SetOutputColumnProperty(output.ID, result.ID, "Expression", expression);
        return result;
    }

    private static string Metadata(IDTSOutputColumn100 column)
    {
        return string.Join(",", new[] { column.Name, column.DataType.ToString(), column.Length.ToString(),
            column.Precision.ToString(), column.Scale.ToString(), column.CodePage.ToString() });
    }

    public static void Create(string root, int rowCount)
    {
        var app = new Application();
        var package = new Package { Name = "UniversalDataRowBaseline", LocaleID = 1033 };
        var inputPath = Path.Combine(root, "input.csv");
        using (var input = new StreamWriter(inputPath, false, Encoding.ASCII))
        {
            for (var i = 1; i <= rowCount; i++)
            {
                input.WriteLine(string.Format(CultureInfo.InvariantCulture, "{0},{1}, part{2} ",
                    i, i % 791 - 395, i % 100));
            }
        }
        var sourceConnection = FlatFile(package, "Input", inputPath,
            new[] { "id", "amount", "label" }, new[] { DataType.DT_I4, DataType.DT_I4, DataType.DT_WSTR },
            new[] { 0, 0, 24 });
        var targetConnection = FlatFile(package, "Observed", Path.Combine(root, "output.csv"),
            new[] { "id", "calculated", "normalized" }, new[] { DataType.DT_I4, DataType.DT_I4, DataType.DT_WSTR },
            new[] { 0, 0, 24 });

        var flow = (TaskHost)package.Executables.Add("STOCK:PipelineTask");
        flow.Name = "Transform rows";
        var pipe = (MainPipe)flow.InnerObject;
        var source = AddComponent(pipe, app, "Flat File Source");
        Connect(source, sourceConnection);
        var derived = AddComponent(pipe, app, "Derived Column");
        var derivedInput = derived.InputCollection[0];
        Attach(pipe, source.OutputCollection[0], derivedInput);
        var virtualInput = derivedInput.GetVirtualInput();
        var inputDesign = derived.Instantiate();
        foreach (IDTSVirtualInputColumn100 column in virtualInput.VirtualInputColumnCollection)
            inputDesign.SetUsageType(derivedInput.ID, virtualInput, column.LineageID, DTSUsageType.UT_READONLY);

        const string number = "([id] % 7 == 0) ? NULL(DT_I4) : (DT_I4)([amount] + 7)";
        const string text = "(DT_WSTR,24)UPPER(TRIM([label]))";
        var ids = virtualInput.VirtualInputColumnCollection;
        var calculated = Derived(derived, "calculated", DataType.DT_I4, 0, number,
            number.Replace("[id]", "#" + ids["id"].LineageID).Replace("[amount]", "#" + ids["amount"].LineageID));
        var normalized = Derived(derived, "normalized", DataType.DT_WSTR, 24, text,
            text.Replace("[label]", "#" + ids["label"].LineageID));

        var target = AddComponent(pipe, app, "Flat File Destination");
        var targetInput = target.InputCollection[0];
        Attach(pipe, derived.OutputCollection[0], targetInput);
        Connect(target, targetConnection);
        var targetDesign = target.Instantiate();
        var targetVirtual = targetInput.GetVirtualInput();
        foreach (IDTSExternalMetadataColumn100 external in targetInput.ExternalMetadataColumnCollection)
        {
            var lineage = targetVirtual.VirtualInputColumnCollection[external.Name].LineageID;
            var column = targetDesign.SetUsageType(targetInput.ID, targetVirtual, lineage, DTSUsageType.UT_READONLY);
            targetDesign.MapInputColumn(targetInput.ID, column.ID, external.ID);
        }
        app.SaveToXml(Path.Combine(root, "baseline.dtsx"), package, null);
        File.WriteAllLines(Path.Combine(root, "metadata.csv"), new[] { Metadata(calculated), Metadata(normalized) });
    }
}
