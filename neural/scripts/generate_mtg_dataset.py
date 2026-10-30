#!/usr/bin/env python3
"""
Generate Magic: The Gathering judge training data.
Covers comprehensive rules, stack interactions, tournament scenarios.

Based on official MTG Comprehensive Rules.
"""

import json
import random
import argparse
from typing import List, Dict
from dataclasses import dataclass


@dataclass
class MTGRule:
    """Represents an MTG rule."""
    number: str
    text: str
    category: str


class MTGDatasetGenerator:
    """Generate MTG judge training data."""
    
    def __init__(self):
        self.rules = self.load_comprehensive_rules()
        self.common_scenarios = []
        self.stack_interactions = []
        self.tournament_rulings = []
        self.seen_outputs = set()  # Track unique outputs
        
    def load_comprehensive_rules(self) -> List[MTGRule]:
        """
        Load comprehensive rules.
        
        In production, this would parse the official CR document.
        For now, we'll create key rule categories.
        """
        rules = [
            # Core rules
            MTGRule("100.1", "These are the Magic: The Gathering Comprehensive Rules.", "introduction"),
            MTGRule("100.2", "The Magic Golden Rules: When a card contradicts these rules, the card takes precedence.", "introduction"),
            MTGRule("100.3", "This document is the ultimate authority for Magic gameplay.", "introduction"),
            
            # Game concepts
            MTGRule("102.1", "A game begins with only two players.", "game_concepts"),
            MTGRule("103.1", "At the start of a game, the players determine which one of them will choose who takes the first turn.", "starting"),
            MTGRule("103.7", "Each player draws seven cards.", "starting"),
            
            # Turn structure
            MTGRule("500.1", "A turn consists of five phases: beginning, precombat main, combat, postcombat main, and ending.", "turn_structure"),
            MTGRule("501.1", "The beginning phase consists of three steps: untap, upkeep, and draw.", "turn_structure"),
            MTGRule("506.1", "The combat phase has five steps: beginning of combat, declare attackers, declare blockers, combat damage, and end of combat.", "combat"),
            
            # Stack and priority
            MTGRule("116.1", "Each time a player would get priority, the game first performs all state-based actions simultaneously.", "priority"),
            MTGRule("116.3", "Players can't cast spells or activate abilities when they don't have priority.", "priority"),
            MTGRule("117.1", "Players can cast instants and activate abilities during any phase when they have priority.", "timing"),
            
            # Zones
            MTGRule("400.1", "A zone is a place where objects can be during a game.", "zones"),
            MTGRule("401.1", "The library is where a player draws cards from.", "zones"),
            MTGRule("404.1", "The graveyard is a player's discard pile.", "zones"),
            
            # Permanents
            MTGRule("110.1", "A permanent is a card or token on the battlefield.", "permanents"),
            MTGRule("110.5", "Permanents enter the battlefield untapped, unflipped, face up, and phased in unless a spell or ability says otherwise.", "permanents"),
            
            # Spells
            MTGRule("601.1", "To cast a spell, announce it, choose modes and targets, determine costs, activate mana abilities, pay costs.", "spells"),
            MTGRule("608.1", "Each time all players pass priority in succession, the spell or ability on top of the stack resolves.", "spells"),
            
            # Damage
            MTGRule("120.1", "Objects can deal damage to creatures, planeswalkers, and players.", "damage"),
            MTGRule("120.3", "Damage dealt to a creature or planeswalker is marked on it until cleanup step.", "damage"),
            
            # Layers
            MTGRule("613.1", "The values of characteristics are determined by applying continuous effects in a series of layers.", "layers"),
        ]
        
        return rules
    
    def generate_rule_questions(self) -> List[Dict]:
        """Generate Q&A about specific rules."""
        examples = []
        
        for rule in self.rules:
            # Direct rule lookup
            examples.append({
                "instruction": "Answer this MTG rules question",
                "input": f"What does rule {rule.number} say?",
                "output": rule.text,
                "category": "rule_lookup"
            })
            
            # Contextual questions
            if "priority" in rule.text.lower():
                examples.append({
                    "instruction": "Answer this MTG rules question",
                    "input": "Can I cast spells when I don't have priority?",
                    "output": f"No. According to rule {rule.number}: {rule.text}",
                    "category": "rule_application"
                })
            
            if "combat" in rule.text.lower():
                examples.append({
                    "instruction": "Answer this MTG rules question",
                    "input": "What are the steps of the combat phase?",
                    "output": f"Rule {rule.number}: {rule.text}",
                    "category": "rule_application"
                })
        
        return examples
    
    def generate_stack_scenarios(self) -> List[Dict]:
        """Generate stack interaction scenarios."""
        scenarios = [
            {
                "instruction": "Resolve this MTG stack interaction",
                "input": "I cast Lightning Bolt targeting your creature. You cast Counterspell targeting my Lightning Bolt. What happens?",
                "output": "Counterspell resolves first (last in, first out). It counters Lightning Bolt. Lightning Bolt is put into your graveyard without resolving. The creature is not dealt damage.",
                "category": "stack_interaction"
            },
            {
                "instruction": "Resolve this MTG stack interaction",
                "input": "I cast Giant Growth on my creature. In response, you cast Murder targeting it. What happens?",
                "output": "Murder resolves first, destroying the creature. Then Giant Growth tries to resolve but its target is illegal, so it's countered by the game rules. The creature dies.",
                "category": "stack_interaction"
            },
            {
                "instruction": "Resolve this MTG stack interaction",
                "input": "I attack with a 2/2 creature. You block with a 3/3. I cast Giant Growth on my creature. What's the result?",
                "output": "Your 2/2 becomes a 5/5 until end of turn. In combat damage, your 5/5 deals 5 damage to their 3/3 (lethal), and their 3/3 deals 3 damage to your creature (non-lethal). Their creature dies, yours survives.",
                "category": "stack_interaction"
            },
            {
                "instruction": "Resolve this MTG stack interaction",
                "input": "Can I tap my creature to activate its ability after it's been declared as an attacker?",
                "output": "Yes, unless the ability requires the creature to tap as a cost. Once declared as an attacker, the creature is attacking even if it becomes tapped. Tapping doesn't remove it from combat.",
                "category": "stack_interaction"
            },
            {
                "instruction": "Resolve this MTG stack interaction",
                "input": "I control a creature with lifelink and deathtouch. It's blocked by a 5/5. How much life do I gain?",
                "output": "Deathtouch means any amount of damage is lethal. Your creature only needs to assign 1 damage to kill the 5/5. However, you gain life equal to the damage dealt, which is still the full amount your creature deals. If it's a 2/2 with deathtouch and lifelink, you gain 2 life.",
                "category": "stack_interaction"
            }
        ]
        
        return scenarios
    
    def generate_tournament_scenarios(self) -> List[Dict]:
        """Generate tournament policy and procedure scenarios."""
        scenarios = [
            {
                "instruction": "Rule on this tournament situation",
                "input": "A player drew 8 cards at the start of the game instead of 7. What's the penalty?",
                "output": "This is a Game Rule Violation (Drawing Extra Cards). The penalty depends on when it's discovered. If caught immediately, it's typically a Warning and the extra card is returned. If discovered later, it could be a Game Loss. Judge investigates intent (honest mistake vs. cheating).",
                "category": "tournament_ruling"
            },
            {
                "instruction": "Rule on this tournament situation",
                "input": "A player accidentally revealed a card from their hand. What happens?",
                "output": "If done accidentally (e.g., card slipped), this is Looking at Extra Cards. Penalty is typically a Warning. The opponent may choose to have the card returned to a random position in hand or library, depending on the situation. Judge determines appropriate remedy.",
                "category": "tournament_ruling"
            },
            {
                "instruction": "Rule on this tournament situation",
                "input": "A player cast a spell without paying its mana cost. What's the penalty?",
                "output": "This is a Game Rule Violation. If caught immediately, back up the game and resolve correctly with a Warning. If game state is too complex to back up, the spell resolves but player receives a Warning. Repeated violations could lead to Game Loss.",
                "category": "tournament_ruling"
            },
            {
                "instruction": "Rule on this tournament situation",
                "input": "Time is called during a match. What happens?",
                "output": "The current turn is turn 0. There are 5 additional turns (1-5). Active player completes turn 0, then turns 1-5 proceed. If match isn't decided after turn 5, the result is determined by game wins. If tied, the match is a draw (unless single elimination).",
                "category": "tournament_ruling"
            },
            {
                "instruction": "Rule on this tournament situation",
                "input": "A player forgot their upkeep trigger. Can they still do it?",
                "output": "Missed triggers are handled based on whether they're detrimental or beneficial. For generally detrimental triggers (e.g., cumulative upkeep), opponent chooses whether it happens. For beneficial triggers, if caught immediately (within the turn), it happens. If caught later, judge decides. Player may receive a Warning.",
                "category": "tournament_ruling"
            }
        ]
        
        return scenarios
    
    def generate_card_interactions(self) -> List[Dict]:
        """Generate common card interaction questions."""
        interactions = [
            {
                "instruction": "Explain this MTG card interaction",
                "input": "If I control Doubling Season and create a token, what happens?",
                "output": "Doubling Season doubles the number of tokens created. If an effect would create one token, you create two instead. This is a replacement effect that applies as the tokens are created.",
                "category": "card_interaction"
            },
            {
                "instruction": "Explain this MTG card interaction",
                "input": "Can I sacrifice a creature to pay a cost if it's being destroyed?",
                "output": "Yes, if you have priority. While a destroy effect is on the stack, you can sacrifice the creature before it resolves. Sacrificing is faster than destruction. Once sacrificed, the destroy effect has no legal target and is countered.",
                "category": "card_interaction"
            },
            {
                "instruction": "Explain this MTG card interaction",
                "input": "What happens if two creatures with lifelink deal damage to each other?",
                "output": "Both players gain life equal to the damage dealt by their creature. Lifelink causes damage dealt by the creature to also cause its controller to gain that much life. This happens simultaneously during combat damage.",
                "category": "card_interaction"
            },
            {
                "instruction": "Explain this MTG card interaction",
                "input": "Can I counter an ability?",
                "output": "Generally no, unless you have a specific spell that counters abilities (like Stifle). Most counterspells only counter spells on the stack, not activated or triggered abilities. Check the card text carefully.",
                "category": "card_interaction"
            }
        ]
        
        return interactions
    
    def generate_keyword_ability_questions(self) -> List[Dict]:
        """Generate questions about keyword abilities."""
        keywords = [
            {
                "instruction": "Explain this MTG keyword ability",
                "input": "What does 'Flying' mean?",
                "output": "Flying is an evasion ability. Creatures with flying can only be blocked by creatures with flying or reach. They can block creatures with or without flying.",
                "category": "keyword_ability"
            },
            {
                "instruction": "Explain this MTG keyword ability",
                "input": "What does 'First Strike' do?",
                "output": "First strike causes the creature to deal its combat damage before creatures without first strike. If your first strike creature kills the blocker, the blocker won't deal damage back to your creature.",
                "category": "keyword_ability"
            },
            {
                "instruction": "Explain this MTG keyword ability",
                "input": "What does 'Hexproof' mean?",
                "output": "Hexproof means the permanent can't be the target of spells or abilities your opponents control. You can still target it. Note: Hexproof doesn't prevent board wipes or effects that don't target.",
                "category": "keyword_ability"
            },
            {
                "instruction": "Explain this MTG keyword ability",
                "input": "How does 'Trample' work?",
                "output": "When a creature with trample deals damage to a blocker, excess damage (beyond lethal) is dealt to the defending player or planeswalker. Assign at least lethal damage to all blockers, then remaining damage tramples over.",
                "category": "keyword_ability"
            },
            {
                "instruction": "Explain this MTG keyword ability",
                "input": "What is 'Vigilance'?",
                "output": "Vigilance means the creature doesn't tap when it attacks. This allows it to attack and still be available to block on your opponent's turn. It's particularly valuable for large creatures.",
                "category": "keyword_ability"
            }
        ]
        
        return keywords
    
    def generate_complex_scenarios(self) -> List[Dict]:
        """Generate complex multi-step scenarios."""
        scenarios = [
            {
                "instruction": "Resolve this complex MTG scenario",
                "input": "I control a 2/2 creature enchanted with +1/+1 from an aura. Opponent casts Oblivion Ring targeting my creature. In response, I sacrifice the creature. What happens?",
                "output": "When you sacrifice the creature in response, Oblivion Ring's ability still resolves but has no legal target. It enters the battlefield but isn't exiling anything. Later, when Oblivion Ring leaves the battlefield, its second ability triggers but does nothing since it never exiled anything.",
                "category": "complex_scenario"
            },
            {
                "instruction": "Resolve this complex MTG scenario",
                "input": "I control Zur the Enchanter (flying, attacks, search for enchantment) and Propaganda (opponents pay 2 to attack). How do these interact?",
                "output": "When Zur attacks, his triggered ability goes on the stack. You can search for an enchantment and put it onto the battlefield. Propaganda doesn't affect Zur's attack since he's already been declared as an attacker. Propaganda only applies when attacks are being declared, not after.",
                "category": "complex_scenario"
            }
        ]
        
        return scenarios
    
    def _deduplicate(self, examples: List[Dict]) -> List[Dict]:
        """Remove duplicate examples based on output content."""
        import hashlib
        unique = []
        
        for ex in examples:
            output_hash = hashlib.md5(ex['output'].encode()).hexdigest()
            if output_hash not in self.seen_outputs:
                self.seen_outputs.add(output_hash)
                unique.append(ex)
        
        return unique
    
    def generate_dataset(self, num_examples: int = 1000) -> List[Dict]:
        """Generate complete MTG judge training dataset with deduplication."""
        print(f"🎯 Target: {num_examples} unique MTG judge examples\n")
        
        all_examples = []
        
        # Generate different types
        print("  Generating rule questions...")
        rules = self._deduplicate(self.generate_rule_questions())
        all_examples.extend(rules)
        print(f"    ✓ {len(rules)} unique rule questions")
        
        print("  Generating stack scenarios...")
        stack = self._deduplicate(self.generate_stack_scenarios())
        all_examples.extend(stack)
        print(f"    ✓ {len(stack)} unique stack scenarios")
        
        print("  Generating tournament scenarios...")
        tournament = self._deduplicate(self.generate_tournament_scenarios())
        all_examples.extend(tournament)
        print(f"    ✓ {len(tournament)} unique tournament scenarios")
        
        print("  Generating card interactions...")
        cards = self._deduplicate(self.generate_card_interactions())
        all_examples.extend(cards)
        print(f"    ✓ {len(cards)} unique card interactions")
        
        print("  Generating keyword abilities...")
        keywords = self._deduplicate(self.generate_keyword_ability_questions())
        all_examples.extend(keywords)
        print(f"    ✓ {len(keywords)} unique keyword questions")
        
        print("  Generating complex scenarios...")
        complex_ex = self._deduplicate(self.generate_complex_scenarios())
        all_examples.extend(complex_ex)
        print(f"    ✓ {len(complex_ex)} unique complex scenarios")
        
        # If we still need more, generate additional rounds with variations
        attempts = 0
        while len(all_examples) < num_examples and attempts < 5:
            attempts += 1
            print(f"  Generating additional examples (round {attempts})...")
            more_stack = self._deduplicate(self.generate_stack_scenarios())
            more_cards = self._deduplicate(self.generate_card_interactions())
            all_examples.extend(more_stack)
            all_examples.extend(more_cards)
        
        # Shuffle and limit
        random.shuffle(all_examples)
        final = all_examples[:num_examples]
        
        print(f"\n✅ Generated {len(final)} unique examples")
        print(f"   Deduplication: {len(self.seen_outputs)} unique outputs")
        
        return final


def main():
    parser = argparse.ArgumentParser(
        description="Generate MTG judge training dataset"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/mtg_judge.jsonl",
        help="Output file path"
    )
    parser.add_argument(
        "--num-examples",
        type=int,
        default=1000,
        help="Number of examples to generate"
    )
    parser.add_argument(
        "--include-advanced",
        action="store_true",
        help="Include advanced judge scenarios"
    )
    
    args = parser.parse_args()
    
    generator = MTGDatasetGenerator()
    examples = generator.generate_dataset(args.num_examples)
    
    # Save to file
    import os
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    with open(args.output, 'w') as f:
        for example in examples:
            f.write(json.dumps(example) + '\n')
    
    print(f"✓ Generated {len(examples)} examples")
    print(f"✓ Saved to {args.output}")
    
    # Statistics
    from collections import defaultdict
    by_category = defaultdict(int)
    for ex in examples:
        by_category[ex.get('category', 'unknown')] += 1
    
    print("\nDataset Statistics:")
    for category, count in sorted(by_category.items()):
        print(f"  {category}: {count}")
    
    print("\nNext steps:")
    print(f"  1. Review: cat {args.output} | head -20")
    print(f"  2. Validate: python scripts/validate_dataset.py {args.output}")
    print(f"  3. Train: python train.py --dataset {args.output} --output_dir models/mtg-judge")
    print(f"  4. Export: python export_to_gguf.py --model_dir models/mtg-judge --output mtg-judge.gguf")


if __name__ == "__main__":
    main()
