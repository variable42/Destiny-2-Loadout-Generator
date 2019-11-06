val args = sc.getConf.get("spark.driver.args").split("\\s+")
val characterClass = args(0).toLowerCase.substring(0,1) match {
    case "t" => "Titan"
    case "h" => "Hunter"
    case "w" => "Warlock"
}

val filterTier = args(1)

val classItemName = characterClass match {
    case "Titan" => "Titan Mark"
    case "Hunter" => "Hunter Cloak"
    case "Warlock" => "Warlock Bond"
}

val df = spark.read.format("csv").
    option("header", "true").
    load("destinyArmor.csv")

val classArmor = df.filter($"Equippable" === characterClass).
    select("Name","Type","Id","Tier","Masterwork Tier","Mobility","Resilience","Recovery","Discipline","Intellect","Strength","Perks 0","Perks 1","Perks 2").
    withColumn("Mobility", when(($"Perks 0" === "Mobility Mod*") && ($"Type" !== classItemName), ($"Mobility" - 10)).otherwise($"Mobility")).
    withColumn("Resilience", when(($"Perks 0" === "Resilience Mod*") && ($"Type" !== classItemName), ($"Resilience" - 10)).otherwise($"Resilience")).
    withColumn("Recovery", when(($"Perks 0" === "Recovery Mod*") && ($"Type" !== classItemName), ($"Recovery" - 10)).otherwise($"Recovery")).
    withColumn("Discipline", when(($"Perks 0" === "Discipline Mod*") && ($"Type" !== classItemName), ($"Discipline" - 10)).otherwise($"Discipline")).
    withColumn("Intellect", when(($"Perks 0" === "Intellect Mod*") && ($"Type" !== classItemName), ($"Intellect" - 10)).otherwise($"Intellect")).
    withColumn("Strength", when(($"Perks 0" === "Strength Mod*") && ($"Type" !== classItemName), ($"Strength" - 10)).otherwise($"Strength"))

val helmets = classArmor.filter($"Type" === "Helmet").
    withColumnRenamed("Name", "Name_H").
    withColumnRenamed("Type", "Type_H").
    withColumnRenamed("Id", "Id_H").
    withColumn("Exotic_H", when($"Tier" === "Exotic", 1).otherwise(0)).
    withColumnRenamed("Mobility", "Mobility_H").
    withColumnRenamed("Resilience", "Resilience_H").
    withColumnRenamed("Recovery", "Recovery_H").
    withColumnRenamed("Discipline", "Discipline_H").
    withColumnRenamed("Intellect", "Intellect_H").
    withColumnRenamed("Strength", "Strength_H").
    withColumn("Mobility_MW_H", when($"Masterwork Tier" === 10, $"Mobility_H").otherwise($"Mobility_H" + 2)).
    withColumn("Resilience_MW_H", when($"Masterwork Tier" === 10, $"Resilience_H").otherwise($"Resilience_H" + 2)).
    withColumn("Recovery_MW_H", when($"Masterwork Tier" === 10, $"Recovery_H").otherwise($"Recovery_H" + 2)).
    withColumn("Discipline_MW_H", when($"Masterwork Tier" === 10, $"Discipline_H").otherwise($"Discipline_H" + 2)).
    withColumn("Intellect_MW_H", when($"Masterwork Tier" === 10, $"Intellect_H").otherwise($"Intellect_H" + 2)).
    withColumn("Strength_MW_H", when($"Masterwork Tier" === 10, $"Strength_H").otherwise($"Strength_H" + 2)).
    drop("Perks 0","Perks 1","Perks 2").
    drop("Masterwork Tier").
    drop("Tier")


val gauntlets = classArmor.filter($"Type" === "Gauntlets").
    withColumnRenamed("Name", "Name_G").
    withColumnRenamed("Type", "Type_G").
    withColumnRenamed("Id", "Id_G").
    withColumn("Exotic_G", when($"Tier" === "Exotic", 1).otherwise(0)).
    withColumnRenamed("Mobility", "Mobility_G").
    withColumnRenamed("Resilience", "Resilience_G").
    withColumnRenamed("Recovery", "Recovery_G").
    withColumnRenamed("Discipline", "Discipline_G").
    withColumnRenamed("Intellect", "Intellect_G").
    withColumnRenamed("Strength", "Strength_G").
    withColumn("Mobility_MW_G", when($"Masterwork Tier" === 10, $"Mobility_G").otherwise($"Mobility_G" + 2)).
    withColumn("Resilience_MW_G", when($"Masterwork Tier" === 10, $"Resilience_G").otherwise($"Resilience_G" + 2)).
    withColumn("Recovery_MW_G", when($"Masterwork Tier" === 10, $"Recovery_G").otherwise($"Recovery_G" + 2)).
    withColumn("Discipline_MW_G", when($"Masterwork Tier" === 10, $"Discipline_G").otherwise($"Discipline_G" + 2)).
    withColumn("Intellect_MW_G", when($"Masterwork Tier" === 10, $"Intellect_G").otherwise($"Intellect_G" + 2)).
    withColumn("Strength_MW_G", when($"Masterwork Tier" === 10, $"Strength_G").otherwise($"Strength_G" + 2)).
    drop("Perks 0","Perks 1","Perks 2").
    drop("Masterwork Tier").
    drop("Tier")


val chests = classArmor.filter($"Type" === "Chest Armor").
    withColumnRenamed("Name", "Name_C").
    withColumnRenamed("Type", "Type_C").
    withColumnRenamed("Id", "Id_C").
    withColumn("Exotic_C", when($"Tier" === "Exotic", 1).otherwise(0)).
    withColumnRenamed("Mobility", "Mobility_C").
    withColumnRenamed("Resilience", "Resilience_C").
    withColumnRenamed("Recovery", "Recovery_C").
    withColumnRenamed("Discipline", "Discipline_C").
    withColumnRenamed("Intellect", "Intellect_C").
    withColumnRenamed("Strength", "Strength_C").
    withColumn("Mobility_MW_C", when($"Masterwork Tier" === 10, $"Mobility_C").otherwise($"Mobility_C" + 2)).
    withColumn("Resilience_MW_C", when($"Masterwork Tier" === 10, $"Resilience_C").otherwise($"Resilience_C" + 2)).
    withColumn("Recovery_MW_C", when($"Masterwork Tier" === 10, $"Recovery_C").otherwise($"Recovery_C" + 2)).
    withColumn("Discipline_MW_C", when($"Masterwork Tier" === 10, $"Discipline_C").otherwise($"Discipline_C" + 2)).
    withColumn("Intellect_MW_C", when($"Masterwork Tier" === 10, $"Intellect_C").otherwise($"Intellect_C" + 2)).
    withColumn("Strength_MW_C", when($"Masterwork Tier" === 10, $"Strength_C").otherwise($"Strength_C" + 2)).
    drop("Perks 0","Perks 1","Perks 2").
    drop("Masterwork Tier").
    drop("Tier")


val legs = classArmor.filter($"Type" === "Leg Armor").
    withColumnRenamed("Name", "Name_L").
    withColumnRenamed("Type", "Type_L").
    withColumnRenamed("Id", "Id_L").
    withColumn("Exotic_L", when($"Tier" === "Exotic", 1).otherwise(0)).
    withColumnRenamed("Mobility", "Mobility_L").
    withColumn("Mobility_L", when(($"Perks 1" === "Traction*" || $"Perks 2" === "Traction*"), ($"Mobility_L" - 5)).otherwise($"Mobility_L")).
    withColumnRenamed("Resilience", "Resilience_L").
    withColumnRenamed("Recovery", "Recovery_L").
    withColumnRenamed("Discipline", "Discipline_L").
    withColumnRenamed("Intellect", "Intellect_L").
    withColumnRenamed("Strength", "Strength_L").
    withColumn("Mobility_MW_L", when($"Masterwork Tier" === 10, $"Mobility_L").otherwise($"Mobility_L" + 2)).
    withColumn("Resilience_MW_L", when($"Masterwork Tier" === 10, $"Resilience_L").otherwise($"Resilience_L" + 2)).
    withColumn("Recovery_MW_L", when($"Masterwork Tier" === 10, $"Recovery_L").otherwise($"Recovery_L" + 2)).
    withColumn("Discipline_MW_L", when($"Masterwork Tier" === 10, $"Discipline_L").otherwise($"Discipline_L" + 2)).
    withColumn("Intellect_MW_L", when($"Masterwork Tier" === 10, $"Intellect_L").otherwise($"Intellect_L" + 2)).
    withColumn("Strength_MW_L", when($"Masterwork Tier" === 10, $"Strength_L").otherwise($"Strength_L" + 2)).
    drop("Perks 0","Perks 1","Perks 2").
    drop("Masterwork Tier").
    drop("Tier")


val classItems = classArmor.filter($"Type" === classItemName).
    withColumnRenamed("Name", "Name_CL").
    withColumnRenamed("Type", "Type_CL").
    withColumnRenamed("Id", "Id_CL").
    withColumnRenamed("Mobility", "Mobility_CL").
    withColumn("Mobility_CL", when($"Masterwork Tier" === 10, 2).otherwise($"Mobility_CL")).
    withColumnRenamed("Resilience", "Resilience_CL").
    withColumn("Resilience_CL", when($"Masterwork Tier" === 10, 2).otherwise($"Resilience_CL")).
    withColumnRenamed("Recovery", "Recovery_CL").
    withColumn("Recovery_CL", when($"Masterwork Tier" === 10, 2).otherwise($"Recovery_CL")).
    withColumnRenamed("Discipline", "Discipline_CL").
    withColumn("Discipline_CL", when($"Masterwork Tier" === 10, 2).otherwise($"Discipline_CL")).
    withColumnRenamed("Intellect", "Intellect_CL").
    withColumn("Intellect_CL", when($"Masterwork Tier" === 10, 2).otherwise($"Intellect_CL")).
    withColumnRenamed("Strength", "Strength_CL").
    withColumn("Strength_CL", when($"Masterwork Tier" === 10, 2).otherwise($"Strength_CL")).
    withColumn("Mobility_MW_CL", when($"Masterwork Tier" === 10, $"Mobility_CL").otherwise($"Mobility_CL" + 2)).
    withColumn("Resilience_MW_CL", when($"Masterwork Tier" === 10, $"Resilience_CL").otherwise($"Resilience_CL" + 2)).
    withColumn("Recovery_MW_CL", when($"Masterwork Tier" === 10, $"Recovery_CL").otherwise($"Recovery_CL" + 2)).
    withColumn("Discipline_MW_CL", when($"Masterwork Tier" === 10, $"Discipline_CL").otherwise($"Discipline_CL" + 2)).
    withColumn("Intellect_MW_CL", when($"Masterwork Tier" === 10, $"Intellect_CL").otherwise($"Intellect_CL" + 2)).
    withColumn("Strength_MW_CL", when($"Masterwork Tier" === 10, $"Strength_CL").otherwise($"Strength_CL" + 2)).
    drop("Perks 0","Perks 1","Perks 2").
    drop("Masterwork Tier").
    drop("Tier")


val joined = helmets.
    crossJoin(gauntlets).
    crossJoin(chests).
    crossJoin(legs).
    crossJoin(classItems).
    withColumn("Exotics", $"Exotic_H" + $"Exotic_G" + $"Exotic_C" + $"Exotic_L").
    filter($"Exotics" <= 1).
    drop("Exotics","Exotic_H","Exotic_G","Exotic_C","Exotic_L").
    withColumn("Mobility", $"Mobility_H" + $"Mobility_G" + $"Mobility_C" + $"Mobility_L" + $"Mobility_CL").
    withColumn("Resilience", $"Resilience_H" + $"Resilience_G" + $"Resilience_C" + $"Resilience_L" + $"Resilience_CL").
    withColumn("Recovery", $"Recovery_H" + $"Recovery_G" + $"Recovery_C" + $"Recovery_L" + $"Recovery_CL").
    withColumn("Discipline", $"Discipline_H" + $"Discipline_G" + $"Discipline_C" + $"Discipline_L" + $"Discipline_CL").
    withColumn("Intellect", $"Intellect_H" + $"Intellect_G" + $"Intellect_C" + $"Intellect_L" + $"Intellect_CL").
    withColumn("Strength", $"Strength_H" + $"Strength_G" + $"Strength_C" + $"Strength_L" + $"Strength_CL").
    withColumn("Wasted", ($"Mobility" % 10) + ($"Resilience" % 10) + ($"Recovery" % 10) + ($"Discipline" % 10) + ($"Intellect" % 10) + ($"Strength" % 10)).
    withColumn("Tiers", (($"Mobility" + $"Resilience" + $"Recovery" + $"Discipline" + $"Intellect" + $"Strength" - $"Wasted") / 10)).
    withColumn("Tiers", format_number($"Tiers", 0)).
    filter($"Tiers" >= filterTier).
    withColumn("Mobility_MW", $"Mobility_MW_H" + $"Mobility_MW_G" + $"Mobility_MW_C" + $"Mobility_MW_L" + $"Mobility_MW_CL").
    withColumn("Resilience_MW", $"Resilience_MW_H" + $"Resilience_MW_G" + $"Resilience_MW_C" + $"Resilience_MW_L" + $"Resilience_MW_CL").
    withColumn("Recovery_MW", $"Recovery_MW_H" + $"Recovery_MW_G" + $"Recovery_MW_C" + $"Recovery_MW_L" + $"Recovery_MW_CL").
    withColumn("Discipline_MW", $"Discipline_MW_H" + $"Discipline_MW_G" + $"Discipline_MW_C" + $"Discipline_MW_L" + $"Discipline_MW_CL").
    withColumn("Intellect_MW", $"Intellect_MW_H" + $"Intellect_MW_G" + $"Intellect_MW_C" + $"Intellect_MW_L" + $"Intellect_MW_CL").
    withColumn("Strength_MW", $"Strength_MW_H" + $"Strength_MW_G" + $"Strength_MW_C" + $"Strength_MW_L" + $"Strength_MW_CL").
    drop("Mobility_MW_H","Mobility_MW_G","Mobility_MW_C","Mobility_MW_L","Mobility_MW_CL").
    drop("Resilience_MW_H","Resilience_MW_G","Resilience_MW_C","Resilience_MW_L","Resilience_MW_CL").
    drop("Recovery_MW_H","Recovery_MW_G","Recovery_MW_C","Recovery_MW_L","Recovery_MW_CL").
    drop("Discipline_MW_H","Discipline_MW_G","Discipline_MW_C","Discipline_MW_L","Discipline_MW_CL").
    drop("Intellect_MW_H","Intellect_MW_G","Intellect_MW_C","Intellect_MW_L","Intellect_MW_CL").
    drop("Strength_MW_H","Strength_MW_G","Strength_MW_C","Strength_MW_L","Strength_MW_CL").
    withColumn("Wasted_MW", ($"Mobility_MW" % 10) + ($"Resilience_MW" % 10) + ($"Recovery_MW" % 10) + ($"Discipline_MW" % 10) + ($"Intellect_MW" % 10) + ($"Strength_MW" % 10)).
    withColumn("Tiers_MW", (($"Mobility_MW" + $"Resilience_MW" + $"Recovery_MW" + $"Discipline_MW" + $"Intellect_MW" + $"Strength_MW" - $"Wasted_MW") / 10))

joined.write.format("csv").option("header", true).save(characterClass)

println()
println("Complete. Press any key to exit")
scala.io.StdIn.readLine()

sys.exit(0)